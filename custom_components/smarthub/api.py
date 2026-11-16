"""SmartHub API client for Home Assistant integration."""
from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, List
from enum import StrEnum

import aiohttp
from aiohttp import ClientTimeout, ClientError
import pyotp

from .const import (
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
    SESSION_TIMEOUT,
    ELECTRIC_SERVICE,
    SUPPORTED_SERVICES,
    FALLBACK_SERVICES,
    METER_NAME,
)
from .exceptions import (
    SmartHubAuthenticationError,
    SmartHubConnectionError,
    SmartHubDataError,
    SmartHubError as SmartHubAPIError,
)
from .utils import sanitize_host, parse_epoch_set_timezone

_LOGGER = logging.getLogger(__name__)

# Payloads above this size are decoded in an executor thread (a 31-day 15-minute
# HOURLY response is ~2.5 MB).
_JSON_EXECUTOR_THRESHOLD = 256 * 1024
_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
PENDING_POLLS = 6
PENDING_DELAY = 3  # seconds
MAX_INLINE_RETRY_AFTER = 60  # seconds; longer server-requested waits end the call


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    """Parse Retry-After (delta-seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        from email.utils import parsedate_to_datetime
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class ParseType(StrEnum):
    FORWARD = "FORWARD"
    TOTAL = "TOTAL"      # upstream #71: some co-ops label consumption TOTAL
    NET = "NET"
    RETURN = "RETURN"
    REVERSE = "REVERSE"  # upstream #73: export/generation channel (values are <= 0)

class Aggregation(StrEnum):
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    MONTHLY = "MONTHLY"

    @property
    def label(self) -> str:
        """Return human readable label."""
        if self == Aggregation.HOURLY:
            return "Hourly"
        if self == Aggregation.DAILY:
            return "Daily"
        if self == Aggregation.MONTHLY:
            return "Monthly"
        return "Unknown"

    @property
    def suffix(self) -> str:
        """Return statistic ID suffix."""
        if self == Aggregation.HOURLY:
            return ""
        if self == Aggregation.DAILY:
            return "_daily"
        if self == Aggregation.MONTHLY:
            return "_monthly"
        return "_unknown"

    @property
    def period(self) -> str:
        """Return statistic period."""
        if self == Aggregation.HOURLY:
            return "hour"
        if self == Aggregation.DAILY:
            return "day"
        if self == Aggregation.MONTHLY:
            return "month"
        return "unknown"

class SmartHubLocation():
    """Smarthub Location object - contains location_id, location_description, etc"""

    def __init__(
        self,
        id: str,
        service: str,
        description: str,
        provider: str,
    )  -> None:
        """Initialize the SmartHubLocation."""
        self.id = id
        self.service = service
        self.description = description
        self.provider = provider

    def __str__(self):
        return f"[SmartHubLocation: '{self.id}' '{self.service}' '{self.description}']"

class SmartHubAPI:
    """Class to interact with the SmartHub API."""

    def __init__(
        self,
        email: str,
        password: str,
        account_id: str,
        timezone: str,
        mfa_totp: str,
        host: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        """Initialize the SmartHub API client."""
        self.email = email
        self.password = password
        self.account_id = account_id
        self.timezone = timezone
        self.mfa_totp = mfa_totp
        self.host = sanitize_host(host)
        self.timeout = timeout
        self.token: Optional[str] = None
        self.primary_username: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_created_at: Optional[datetime] = None
        # Server-requested quiet period (Retry-After); no request of any kind before it.
        self.not_before: Optional[datetime] = None

    def parse_usage_series(self, usage_data: List[Dict], parseType: ParseType = ParseType.FORWARD) -> List[Dict]:
        parsed_data = []
        _LOGGER.debug(f"First 10 entries of usage data: {usage_data[:10]}")
        for usage in usage_data:
            # convert microseconds to timestmap -> read data as if it was in provider TZ
            event_time = parse_epoch_set_timezone(usage.get("x") / 1000.0, ZoneInfo(self.timezone))
            # HA stats import wants timestamps only at standard intervals -
            # https://github.com/home-assistant/core/blob/4fef19c7bc7c1f7be827f6c489ad1df232e44906/homeassistant/components/recorder/statistics.py#L2634
             # If the first entry isn't aligned with the top of the hour - treat it as if it was
            if event_time.minute != 0 and len(parsed_data) == 0:
              _LOGGER.warning("Initial usage data is not aligned with top of the hour, inserting a 0 entry at 0 minutes: event_time:%s, %s", event_time, usage.get("x"))
              zero_time = event_time.replace(minute=0)
              parsed_data.append({
                "reading_time" : zero_time,
                "consumption" : 0,
                "raw_timestamp": int(zero_time.replace(tzinfo=timezone.utc).timestamp()*1000), # convert zero_time to UTC, and multiply by 1000
              })

            # If the previous parsed time is in a different hour, and we still don't have a 00 minute - insert a 0 entry
            if len(parsed_data) > 0 and parsed_data[-1]["reading_time"].hour != event_time.hour and event_time.minute != 0:
              _LOGGER.warning("Usage data is not aligned with top of the hour, inserting a 0 entry at 0 minutes: event_time:%s, %s", event_time, usage.get("x"))
              zero_time = event_time.replace(minute=0)
              parsed_data.append({
                "reading_time" : zero_time,
                "consumption" : 0,
                "raw_timestamp": int(zero_time.replace(tzinfo=timezone.utc).timestamp()*1000), # convert zero_time to UTC, and multiply by 1000
              })

            # When doing a normal energy monitoring - never report negative numbers
            # When doing a NET usage, only report negative numbers, but invert them to be positive
            usage_energy = usage.get("y")
            if parseType == ParseType.NET:
              if usage_energy > 0:
                usage_energy = 0
              else:
                usage_energy = abs(usage_energy)
            elif parseType in (ParseType.RETURN, ParseType.REVERSE):
              # Export channels may be reported as negative (REVERSE "Generation").
              usage_energy = abs(usage_energy)
            else:
              usage_energy = max(0,usage_energy)


            if event_time.minute != 0:
              _LOGGER.debug("consolidating sub hour data: %s, %s + %s", event_time, parsed_data[-1]['consumption'], usage.get("y"))
              parsed_data[-1]['consumption'] += usage_energy
              continue

            parsed_data.append({
              "reading_time" : event_time,
              "consumption" : usage_energy,
              "raw_timestamp": usage.get("x"),
            })

        return parsed_data

    def parse_usage(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse the JSON data and extract the last data point for usage.

        Args:
            data: The JSON data as a Python dictionary.

        Returns:
            A dictionary containing the "USAGE" with a list of parsed data and metadata, or an empty dictionary if not found.

        Raises:
            SmartHubDataError: If there's an error parsing the data.
        """
        try:
            parsed_response = {}

            if not isinstance(data, dict):
                raise SmartHubDataError("Invalid data format: expected dictionary")

            # Locate the "ELECTRIC" data
            electric_data = data.get("data", {}).get("ELECTRIC", [])
            if len(electric_data) == 0:
              _LOGGER.warning("No ELECTRIC data found in response")
              _LOGGER.debug(data)

            for entry in electric_data:
                # Find the entry with type "USAGE"
                if entry.get("type","") == "USAGE":
                    _LOGGER.debug("Usage: %s", entry)

                    meters = entry.get("meters", [])
                    forward_series = ""
                    net_series = ""
                    return_series = ""
                    if len(meters) > 2:
                      _LOGGER.debug("%d meter channels in usage data", len(meters))
                    for meter in meters:
                      # assume forward is default if not present
                      flow_direction = str(meter.get("flowDirection", ParseType.FORWARD)).upper()
                      match flow_direction:
                        case ParseType.FORWARD | ParseType.TOTAL:
                          forward_series = meter["seriesId"]
                        case ParseType.NET:
                          net_series = meter["seriesId"]
                        case ParseType.RETURN | ParseType.REVERSE:
                          return_series = meter["seriesId"]
                        case _:
                          _LOGGER.debug("Ignoring meter channel with flow direction %s", flow_direction)

                    series = entry.get("series", [])
                    for serie in series:
                        if serie.get("name", "") == return_series:
                            usage_data = serie.get("data", [])
                            parsed_response["USAGE_RETURN"] = self.parse_usage_series(usage_data, ParseType.RETURN)
                            _LOGGER.debug("Parsed %d items for USAGE_RETURN history", len(parsed_response["USAGE_RETURN"]))

                        # If there is a NetMeter, use that for both Return and Usage (as it combines both).
                        # NOTE - there must always be a FORWARD or NET meter - or the "USAGE" is not being returned.
                        if serie.get("name", "") == (net_series if net_series != "" else forward_series):
                            parsed_response[METER_NAME] = serie.get("name")

                            # Extract the last data point in the "data" array
                            usage_data = serie.get("data", [])
                            parsed_response["USAGE"] = self.parse_usage_series(usage_data)
                            _LOGGER.debug("Parsed %d items for USAGE history", len(parsed_response["USAGE"]))

                            if net_series != "":
                              parsed_response["USAGE_RETURN"] = self.parse_usage_series(usage_data, ParseType.NET)
                              _LOGGER.debug("Parsed %d items for USAGE_RETURN history", len(parsed_response["USAGE_RETURN"]))
                else:
                    _LOGGER.debug("Unknown Usage: %s", entry)

            return parsed_response

        except Exception as e:
            _LOGGER.debug("Error parsing usage data", exc_info=True)
            raise SmartHubDataError(f"Error parsing usage data: {e}") from e

    def parse_locations(self, location_json) -> List[SmartHubLocation]:
        # Response format is structured as a list of dictionaries -
        # each dictionary has the following keys
        #   "account",
        #   "additionalCustomerName",
        #   "address",
        #   "agreementStatus",
        #   "consumerClassCode",
        #   "customer",
        #   "customerName",
        #   "disconnectNonPay",
        #   "email",
        #   "inactive",
        #   "invoiceGroupNumber",
        #   "isAutoPay",
        #   "isDisconnected",
        #   "isMultiService",
        #   "isPendingDisconnect",
        #   "isUnCollectible",
        #   "primaryServiceLocationId",
        #   "providerOrServiceDescription",
        #   "providerToDescription",
        #   "providerToProviderDescription",
        #   "serviceLocationIdToServiceLocationSummary",
        #   "serviceLocationToIndustries",
        #   "serviceLocationToProviders",
        #   "serviceLocationToUserDataServiceLocationSummaries",
        #   "serviceToProviders",
        #   "serviceToServiceDescription",
        #   "services"
        # `serviceLocationToUserDataServiceLocationSummaries` Includes human readable information about the service location.
        # Which is a map of the location_id, to a list of
          #  "activeRateSchedules",
          #  "address",
          #  "description",
          #  "id",
          #  "lastBillPresReadDtTm",
          #  "lastBillPrevReadDtTm",
          #  "location",
          #  "serviceStatus",
          #  "services"

        locations = []
        _LOGGER.debug(location_json)

        for entry in location_json:
          if entry.get("inactive", False): # assume active by default
            continue # Don't include inactive accounts in list

          services = entry.get("services",[])
          serviceToProviders = entry.get("serviceToProviders", {})
          serviceToServiceDescription = entry.get("serviceToServiceDescription",{'ELEC': 'Electric Service'})
          serviceLocationToUserDataServiceLocationSummaries = entry.get("serviceLocationToUserDataServiceLocationSummaries", {})
          providerOrServiceDescription = entry.get("providerToDescription",{})

          # Loop through the locations looking for the service description `ELECTRIC_SERVICE` which maps the service key - usually ELEC, but sometimes 1ELEC
          electric_service_keys = {
                service for service, desc in serviceToServiceDescription.items()
                if isinstance(desc, str) and ELECTRIC_SERVICE.lower() in desc.lower()
          }

          # Some smarthub systems don't return 'Electric Service' as a distinct entity. hsvutil.smarthub.coop returns
          # 'serviceToServiceDescription': {'WATER|NGAS|ELEC|SEWER|TRASH': 'City Utilities'},
          for fallback in FALLBACK_SERVICES:
            if fallback in services:
              electric_service_keys.add(fallback)

          for electric_service in electric_service_keys:
              electrical_providers = serviceToProviders.get(electric_service,["unknown"])
              electrical_provider = electrical_providers[0] if electrical_providers else "unknown"
              for locationID, serviceDescriptions in serviceLocationToUserDataServiceLocationSummaries.items():
                for serviceDescription in serviceDescriptions:
                  # for now only support electric service type
                  if any(service in [electric_service] for service in serviceDescription.get("services",[])):
                    # Try to find a good description
                    description = serviceDescription.get("description", "")

                    locations.append(
                      SmartHubLocation(
                        id=locationID,
                        service=ELECTRIC_SERVICE,
                        description=description,
                        provider=providerOrServiceDescription.get(electrical_provider,electrical_provider),
                      )
                    )

        return locations

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        now = datetime.now()

        # Check if session needs refresh due to age
        if (self._session_created_at and
            (now - self._session_created_at).total_seconds() > SESSION_TIMEOUT):
            _LOGGER.debug("Session timeout reached, refreshing session")
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None
            self._session_created_at = None

        if self._session is None or self._session.closed:
            timeout = ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=aiohttp.TCPConnector(ssl=True, limit=10),
            )
            self._session_created_at = now
            _LOGGER.debug("Created new aiohttp session")
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _refresh_authentication(self) -> None:
        """
        Refresh authentication by clearing session and getting new token.

        This method ensures we start with a clean session state when
        authentication issues occur.
        """
        _LOGGER.debug("Refreshing authentication and session")

        # Close existing session to clear any stale state
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._session_created_at = None

        # Clear the old token
        self.token = None

        # Get a fresh token with a new session
        await self.get_token()

    async def get_token(self) -> str:
        """
        Authenticate and retrieve the token asynchronously.

        Returns:
            The authentication token.

        Raises:
            SmartHubAuthenticationError: If authentication fails.
            SmartHubConnectionError: If there's a connection error.
        """
        auth_url = f"https://{self.host}/services/oauth/auth/v2"
        headers = {
            "Authority": self.host,
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "HomeAssistant SmartHub Integration",
        }

        payload = {
          "password": self.password,
          "userId": self.email,
        }

        if self.mfa_totp != "":
          totp = pyotp.TOTP(self.mfa_totp)
          current_otp = totp.now()
          payload["twoFactorCode"] = current_otp

        _LOGGER.debug("Sending authentication request to: %s", auth_url)
        self._check_not_before("authentication")

        try:
            session = await self._get_session()
            async with session.post(auth_url, headers=headers, data=payload) as response:
                _ = await response.text()
                _LOGGER.debug("Auth response status: %s", response.status)

                if response.status == 401:
                    raise SmartHubAuthenticationError("Invalid credentials")
                elif response.status != 200:
                    requested = _retry_after_seconds(response.headers.get("Retry-After"))
                    if response.status in _RETRYABLE_STATUS and requested is not None:
                        # Throttled at login: no traffic of any kind until the server's deadline.
                        self.not_before = datetime.now(timezone.utc) + timedelta(seconds=requested)
                    raise SmartHubConnectionError(
                        f"Authentication failed with HTTP status: {response.status}"
                        + (f", retry after {requested:.0f}s" if requested is not None else "")
                    )

                try:
                    response_json = await response.json(content_type=None)
                except Exception as e:
                    raise SmartHubConnectionError(f"Invalid authentication response: {type(e).__name__}") from e
                if not isinstance(response_json, dict):
                    raise SmartHubConnectionError("Invalid authentication response")

                self.token = response_json.get("authorizationToken")
                self.primary_username = response_json.get("primaryUsername", self.email)

                if not self.token:
                    # 200 without a token is an unexpected response (maintenance page etc.),
                    # not proof of bad credentials; treat it as transient.
                    raise SmartHubConnectionError("No authorization token in authentication response")

                _LOGGER.debug("Successfully retrieved authentication token")
                return self.token

        except (ClientError, asyncio.TimeoutError) as e:
            raise SmartHubConnectionError(f"Connection error during authentication: {type(e).__name__}: {e}") from e

    def _check_not_before(self, what: str) -> None:
        if self.not_before is not None and datetime.now(timezone.utc) < self.not_before:
            raise SmartHubConnectionError(f"{what}: utility asked to wait until {self.not_before.isoformat()}")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authority": self.host,
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "X-Nisc-Smarthub-Username": self.email,
            "User-Agent": "HomeAssistant SmartHub Integration",
        }

    async def _decode_json(self, response: aiohttp.ClientResponse) -> Any:
        """Decode a JSON body, off the event loop for large payloads."""
        body = await response.read()
        try:
            if len(body) > _JSON_EXECUTOR_THRESHOLD:
                return await asyncio.get_running_loop().run_in_executor(None, json.loads, body)
            return json.loads(body)
        except ValueError as e:
            raise SmartHubDataError(f"Invalid JSON response ({len(body)} bytes)") from e

    async def _authorized_request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        what: str = "request",
    ) -> Any:
        """Send an authorized request and return decoded JSON.

        - reuses the bearer token; on 401 re-authenticates once and retries
        - retries timeouts, connection errors, 408/425/429/5xx with jittered backoff
        """
        reauthed = False
        last_error: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            self._check_not_before(what)
            if not self.token:
                await self._refresh_authentication()
                reauthed = True
            try:
                session = await self._get_session()
                async with session.request(
                    method, url, headers=self._headers(), params=params, json=json_body
                ) as response:
                    if response.status == 401:
                        if reauthed:
                            raise SmartHubAuthenticationError(f"{what}: unauthorized after re-authentication")
                        _LOGGER.debug("%s: token rejected, re-authenticating", what)
                        self.token = None
                        reauthed = True
                        continue
                    if response.status in _RETRYABLE_STATUS:
                        last_error = SmartHubConnectionError(f"{what}: HTTP {response.status}")
                        requested = _retry_after_seconds(response.headers.get("Retry-After"))
                        if requested is not None and requested > MAX_INLINE_RETRY_AFTER:
                            # Honor the server: no traffic of any kind until the deadline.
                            self.not_before = datetime.now(timezone.utc) + timedelta(seconds=requested)
                            raise SmartHubConnectionError(
                                f"{what}: HTTP {response.status}, server asked to retry after {requested:.0f}s"
                            )
                        if requested is not None:
                            await asyncio.sleep(requested + random.random())
                            continue
                        delay = RETRY_DELAY * (2 ** (attempt - 1))
                    elif response.status != 200:
                        raise SmartHubConnectionError(f"{what}: HTTP {response.status}")
                    else:
                        return await self._decode_json(response)
            except (asyncio.TimeoutError, ClientError) as e:
                last_error = SmartHubConnectionError(f"{what}: {type(e).__name__}: {e}")
                delay = RETRY_DELAY * (2 ** (attempt - 1))
            if attempt < MAX_RETRIES:
                delay = min(delay, 60) * (1 + random.random() / 2)
                _LOGGER.debug("%s failed (%s); retry %d in %.1fs", what, last_error, attempt, delay)
                await asyncio.sleep(delay)
        raise last_error or SmartHubConnectionError(f"{what}: failed after {MAX_RETRIES} attempts")

    async def get_service_locations(self) -> List[SmartHubLocation]:
        """Retrieve the account's active electric service locations."""
        user_data_url = f"https://{self.host}/services/secured/user-data"
        if not self.token:
            await self._refresh_authentication()
        response_json = await self._authorized_request(
            "GET", user_data_url, params={"userId": self.primary_username}, what="user-data"
        )
        try:
            return self.parse_locations(response_json)
        except Exception as e:  # malformed structure
            raise SmartHubDataError(f"Invalid user-data response: {type(e).__name__}") from e

    async def fetch_usage(
        self,
        location: SmartHubLocation,
        aggregation: "Aggregation",
        start: datetime,
        end: datetime,
    ) -> Dict[str, Any]:
        """Return the raw COMPLETE usage-poll payload for [start, end] (aware datetimes).

        Raises SmartHubDataError if the utility keeps answering PENDING or returns
        an unexpected status; connection/auth errors propagate.
        """
        poll_url = f"https://{self.host}/services/secured/utility-usage/poll"
        body = {
            "timeFrame": aggregation.value,
            "userId": self.email,
            "screen": "USAGE_EXPLORER",
            "includeDemand": False,
            "serviceLocationNumber": location.id,
            "accountNumber": self.account_id,
            "industries": ["ELECTRIC"],
            "startDateTime": str(int(start.timestamp()) * 1000),
            "endDateTime": str(int(end.timestamp()) * 1000),
        }
        for poll in range(1, PENDING_POLLS + 1):
            payload = await self._authorized_request("POST", poll_url, json_body=body, what="usage-poll")
            status = payload.get("status") if isinstance(payload, dict) else None
            if status == "COMPLETE":
                return payload
            if status != "PENDING":
                raise SmartHubDataError(f"usage-poll: unexpected status {status!r}")
            if poll < PENDING_POLLS:
                await asyncio.sleep(PENDING_DELAY)
        raise SmartHubDataError(f"usage-poll: still PENDING after {PENDING_POLLS} polls")

    async def get_energy_data(self, location, aggregation:Aggregation, start_datetime=None) -> Optional[Dict[str, Any]]:
        """Legacy helper: fetch and parse usage with the v2.2.0 parser."""
        end_datetime = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        if start_datetime is None:
            start_datetime = end_datetime - timedelta(days=30)
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.astimezone()
        payload = await self.fetch_usage(location, aggregation, start_datetime, end_datetime)
        return self.parse_usage(payload)
