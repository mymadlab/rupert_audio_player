"""
Description: Controls rupert audio
"""
import json
import random
import time
from pathlib import Path
from urllib.parse import urlsplit
from loguru import logger
from beartype import beartype
import vlc
from rupert_prosumer import RupertProsumer

class RupertAudioProsumer(RupertProsumer):
	"""
		Description: Audio Synapse class.
		Responsible for:
			1. Basic constructor for connecting/starting
			2. Initiates Kafka cosumer
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		super().__init__(config_file=config_file)
		self.rap = RupertAudioPlayer(
			options="",
			log=self.logger,
			media_policy=self.config.get('audio', {}),
		)

	def _configure_listener_logger(self, topic: str) -> None:
		"""Use the consumer's configured logger for subsequent VLC player operations."""
		del topic
		self.rap.set_logger(self.logger)

	@beartype
	def process_event(self, consumer_message: object) -> None:
		"""
			Description: Initiates events for the requested audio control
			Responsible for:
				1. Converts the messages value to dictionary
				2. Runs the audio control event as a daemon thread
			Requires:
				consumer_message
		"""
		value = consumer_message.value()
		if not isinstance(value, bytes):
			raise ValueError("Audio event payload must be bytes")
		try:
			payload = value.decode("utf-8")
			control_dict = json.loads(payload)
		except (UnicodeDecodeError, json.JSONDecodeError) as error:
			raise ValueError("Audio event must be valid UTF-8 JSON") from error
		if not isinstance(control_dict, dict):
			raise ValueError("Audio event must be a JSON object")
		action_type = control_dict.get('action_type')
		if action_type == 'control':
			self.logger.info("Received control event")
			self.rap.set(control_dict)
		elif action_type == 'status':
			self.logger.info("Status action is not supported; no status publication is configured")
		else:
			raise ValueError("Unknown or missing audio action type")

class RupertAudioPlayer():  # pylint: disable=too-many-instance-attributes
	"""
		Description: Handles playing audio
		Responsible for:
			1. Initiating the VLC Player
			2. Controlling playback
	"""
	@beartype
	def __init__(self, options: str='', log: object = logger,
				 media_policy: dict | None = None) -> None:
		policy = {} if media_policy is None else media_policy
		if not isinstance(policy, dict):
			raise TypeError("Audio media policy must be a dictionary")
		roots = policy.get('allowed_media_roots', [])
		schemes = policy.get('allowed_url_schemes', [])
		hosts = policy.get('allowed_url_hosts', [])
		for name, values in (('allowed_media_roots', roots), ('allowed_url_schemes', schemes),
							 ('allowed_url_hosts', hosts)):
			if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
				raise ValueError(f"{name} must be a list of non-empty strings")
		if any(scheme not in ('http', 'https') for scheme in schemes):
			raise ValueError("Only http and https URL schemes may be allowed")
		if any(host != host.lower() or any(char in host for char in '/:@?#') for host in hosts):
			raise ValueError("Allowed URL hosts must be lowercase hostnames without ports")
		self.media_roots = tuple(Path(root).expanduser().resolve() for root in roots)
		self.url_schemes = frozenset(schemes)
		self.url_hosts = frozenset(hosts)
		self.player = vlc.Instance(options)
		if self.player is None:
			raise RuntimeError("Unable to initialize VLC")
		self.logger = log
		self.media_list = None
		self.media_list_player = None
		self.media = None
		self.control_dict = None
		self.eos = None

	@beartype
	def set_logger(self, log: object) -> None:
		"""Replace the logger used by VLC playback operations."""
		self.logger = log

	@beartype
	def status(self) -> dict[str, str | int | bool | list[str] | None]:
		# pylint: disable=too-many-branches,too-many-nested-blocks
		"""
			Description: Gets the current status of the player
			Responsible for:
				1. Returning a dictionary with the current status of the player
		"""
		current_media = None
		current_track_mrl = None
		track_index = -1
		media_list_mrls: list[str] = []

		if self.media_list_player:
			media_player = self.media_list_player.get_media_player()
			if media_player is None:
				raise RuntimeError("VLC media player unavailable")
			try:
				current_media = media_player.get_media()
				if current_media:
					try:
						current_track_mrl = current_media.get_mrl()
						if self.media_list:
							self.media_list.lock()
							try:
								track_index = self.media_list.index_of_item(current_media)
							finally:
								self.media_list.unlock()
					finally:
						current_media.release()
				volume = media_player.audio_get_volume()
			finally:
				media_player.release()
			if self.media_list:
				self.media_list.lock()
				try:
					for index in range(self.media_list.count()):
						media_item = self.media_list.item_at_index(index)
						if media_item:
							try:
								media_list_mrls.append(media_item.get_mrl())
							finally:
								media_item.release()
				finally:
					self.media_list.unlock()
			state = str(self.media_list_player.get_state())
			try:
				loop_mode = str(self.media_list_player.get_playback_mode())
			except AttributeError:
				loop_mode = "single"
		else:
			state = "stopped"
			loop_mode = "single"
			volume = 0

		status_dict = {
			"state": state,
			"volume": volume,
			"loop": loop_mode,
			"track": current_track_mrl,
			"track_index": track_index,
			"instance_index": track_index,
			"media_list": media_list_mrls,
		}
		return status_dict

	@beartype
	def bye(self) -> None:
		"""Release VLC resources; safe to call again after successful cleanup."""
		try:
			self.__release_media()
		finally:
			if self.player is not None:
				self.player.release()
				self.player = None

	def __release_media(self) -> None:
		"""Release the references owned by this player."""
		errors = []
		if self.media_list_player is not None:
			try:
				self.media_list_player.stop()
			except Exception as error:  # pylint: disable=broad-exception-caught
				errors.append(error)
		for attribute in ('media_list_player', 'media', 'media_list'):
			resource = getattr(self, attribute)
			if resource is not None:
				try:
					resource.release()
				except Exception as error:  # pylint: disable=broad-exception-caught
					errors.append(error)
				else:
					setattr(self, attribute, None)
		if errors:
			raise ExceptionGroup("VLC media cleanup failed", errors)

	def __validate_control(self, control_dict: dict) -> None:
		"""Validate supported control fields before any VLC resource mutation."""
		supported_fields = {
			'action_type', 'play_directory', 'play_track', 'play_tracks',
			'shuffle', 'play', 'volume', 'loop', 'navigate',
		}
		unknown_fields = set(control_dict) - supported_fields
		if unknown_fields:
			raise ValueError("Unknown audio control field(s): " + ', '.join(sorted(unknown_fields)))

		for field in ('play_directory', 'play_track'):
			if field in control_dict and not isinstance(control_dict[field], str):
				raise ValueError(f"{field} must be a string")
		if 'play_tracks' in control_dict:
			tracks = control_dict['play_tracks']
			if (not isinstance(tracks, list) or not tracks
					or not all(isinstance(track, str) for track in tracks)):
				raise ValueError("play_tracks must be a non-empty list of strings")
		if 'shuffle' in control_dict and not isinstance(control_dict['shuffle'], bool):
			raise ValueError("shuffle must be a boolean")
		if 'play' in control_dict and control_dict['play'] not in ('play', 'pause', 'stop'):
			raise ValueError("play must be 'play', 'pause', or 'stop'")
		if 'volume' in control_dict:
			volume = control_dict['volume']
			if not isinstance(volume, int) or isinstance(volume, bool) or not 0 <= volume <= 100:
				raise ValueError("Volume must be an integer from 0 to 100")
		if 'loop' in control_dict and control_dict['loop'] not in ('single', 'repeat', 'loop'):
			raise ValueError("loop must be 'single', 'repeat', or 'loop'")
		if 'navigate' in control_dict and control_dict['navigate'] not in ('next', 'previous'):
			raise ValueError("navigate must be 'next' or 'previous'")

	@beartype
	def set(self, control_dict: dict[str, str | int | bool | list[str]]) -> None:
		"""
		Control the audio player. Requires:
			control_dict = Dictionary detailing what controls to execute
		"""
		if self.player is None:
			raise RuntimeError("Audio player is closed")

		self.__validate_control(control_dict)
		self.control_dict = control_dict.copy()

		if 'play_directory' in self.control_dict:
			self.control_dict['play_tracks'] = self.__get_directory_tracks(
				self.control_dict['play_directory']
			)

		elif 'play_track' in self.control_dict and 'play_tracks' not in self.control_dict:
			self.control_dict['play_tracks'] = [self.control_dict['play_track']]

		if 'play_tracks' in self.control_dict:
			self.__set_media()

		if 'play' in self.control_dict:
			self.__play_stop_pause()
			time.sleep(1) # Required in the event we are also updating loop
		if 'volume' in self.control_dict:
			self.__set_volume()

		if 'loop' in self.control_dict:
			self.__set_loop()

		if 'navigate' in self.control_dict:
			self.__navigate()

## Private methods

	@beartype
	def __get_directory_tracks(self, directory: str) -> list[str]:
		"""
			Description: Gets the tracks in a directory
			Responsible for:
				1. Returning a list of tracks in a directory
		"""
		audio_extensions = {
			'.aac', '.aiff', '.alac', '.flac', '.m4a', '.mp3', '.ogg', '.opus', '.wav', '.wma'
		}
		directory_path = self.__allowed_path(directory)

		if not directory_path.exists() or not directory_path.is_dir():
			raise ValueError(f"Invalid play_directory: {directory}")

		tracks = [
			str(self.__allowed_path(str(path)))
			for path in sorted(directory_path.iterdir())
			if path.is_file() and path.suffix.lower() in audio_extensions
		]

		if not tracks:
			raise ValueError(f"No audio files found in play_directory: {directory}")

		return tracks

	def __allowed_path(self, value: str) -> Path:
		"""Resolve symlinks before checking configured roots."""
		path = Path(value).expanduser().resolve()
		if not any(path.is_relative_to(root) for root in self.media_roots):
			raise ValueError("Media path is outside configured roots")
		return path

	def __allowed_media(self, value: str) -> str:
		"""Allow local files or explicitly configured initial HTTP(S) hosts."""
		if any(ord(char) < 32 or ord(char) == 127 for char in value):
			raise ValueError("Invalid media identifier")
		parsed = urlsplit(value)
		if parsed.scheme:
			if (parsed.scheme not in self.url_schemes or parsed.hostname not in self.url_hosts
					or parsed.username is not None or parsed.port is not None or not parsed.netloc):
				raise ValueError("Media URL is not allowed")
			return value
		path = self.__allowed_path(value)
		if not path.is_file():
			raise ValueError("Media file does not exist")
		return str(path)

	def __require_playlist(self) -> None:
		"""Ensure VLC playback resources exist before applying a control."""
		if self.media_list_player is None:
			raise RuntimeError("No VLC playlist is initialized")

	@beartype
	def __navigate(self) -> None:
		# pylint: disable=too-many-branches
		self.__require_playlist()
		media_player = self.media_list_player.get_media_player()
		if media_player is None:
			raise RuntimeError("VLC media player unavailable")
		try:
			current = media_player.get_media()
			if current is None:
				raise RuntimeError("No current media")
			try:
				self.media_list.lock()
				try:
					media_list_count = self.media_list.count()
					media_index = self.media_list.index_of_item(current)
				finally:
					self.media_list.unlock()
			finally:
				current.release()
		finally:
			media_player.release()
		self.logger.debug(f"Media list count: {media_list_count} media index: {media_index}")
		if self.control_dict['navigate'] == 'next':
			if media_index + 1 < media_list_count: # If not on the last track go to the next one
				self.logger.info("Next track")
				result = self.media_list_player.next()
			else: # Else reset to the initial track
				self.logger.info("Initial Track")
				result = self.media_list_player.play_item_at_index(0)
		elif self.control_dict['navigate'] == 'previous':
			if media_index - 1 >= 0: # If not on the first track go to the previous one
				self.logger.info("Previous track")
				result = self.media_list_player.previous()
			else: # Else reset to the initial track
				self.logger.info("Initial Track")
				result = self.media_list_player.play_item_at_index(0)
		else:
			raise ValueError("Unknown navigation command")
		if result == -1:
			raise RuntimeError("VLC navigation failed")

	@beartype
	def __play_stop_pause(self) -> None:
		"""
			Starts, stops, pauses, and resumes the player
		"""
		self.__require_playlist()
		self.logger.info("Setting playback state")
		if self.control_dict['play'] == 'stop':
			self.__stop()
		elif self.control_dict['play'] == 'play':
			self.media_list_player.play()
		elif self.control_dict['play'] == 'pause': # will resume if already paused
			self.media_list_player.pause()
		else:
			raise ValueError("Unknown play status")

	@beartype
	def __set_loop(self) -> None:
		"""
			Manipulates looping
		"""
		self.__require_playlist()
		self.logger.info("Setting loop mode")
		# current
		if self.control_dict['loop'] == 'repeat':
			self.media_list_player.set_playback_mode(vlc.PlaybackMode.repeat)
		# play list
		elif self.control_dict['loop'] == 'loop':
			self.media_list_player.set_playback_mode(vlc.PlaybackMode.loop)
		# end loop
		elif self.control_dict['loop'] == "single":
			self.media_list_player.set_playback_mode(vlc.PlaybackMode.default)
		else:
			raise ValueError("Unknown loop mode")

	@beartype
	def __set_media(self) -> None:
		"""
			Sets the playing media
		"""
		tracks = self.control_dict['play_tracks']
		if (not isinstance(tracks, list) or not tracks
				or not all(isinstance(item, str) for item in tracks)):
			raise ValueError("play_tracks must be a non-empty list of strings")
		tracks = [self.__allowed_media(track) for track in tracks]
		if self.control_dict.get('shuffle', False):
			random.shuffle(tracks)
		self.__release_media()
		try:
			self.media_list = self.player.media_list_new()
			if self.media_list is None:
				raise RuntimeError("Unable to create VLC media list")
			self.media_list_player = self.player.media_list_player_new()
			if self.media_list_player is None:
				raise RuntimeError("Unable to create VLC list player")
			for track in tracks:
				media = self.player.media_new(track)
				if media is None:
					raise RuntimeError("Unable to create VLC media")
				try:
					self.media_list.lock()
					try:
						result = self.media_list.add_media(media)
					finally:
						self.media_list.unlock()
					if result != 0:
						raise RuntimeError("Unable to add VLC media")
				except BaseException:
					media.release()
					raise
				if self.media is not None:
					try:
						self.media.release()
					except BaseException:
						media.release()
						raise
				self.media = media
			self.media_list_player.set_media_list(self.media_list)
		except BaseException:
			self.__release_media()
			raise

	@beartype
	def __set_volume(self) -> None:
		"""
			Adjusts the volume
		"""
		self.__require_playlist()
		self.logger.info("Setting volume")
		volume = self.control_dict['volume']
		if not isinstance(volume, int) or isinstance(volume, bool) or not 0 <= volume <= 100:
			raise ValueError("Volume must be an integer from 0 to 100")
		media_player = self.media_list_player.get_media_player()
		if media_player is None:
			raise RuntimeError("VLC media player unavailable")
		try:
			if media_player.audio_set_volume(volume) != 0:
				raise RuntimeError("VLC volume update failed")
		finally:
			media_player.release()

	@beartype
	def __stop(self) -> None:
		"""
			Stops the player
		"""
		if self.media_list_player is None:
			raise RuntimeError("No VLC playlist is initialized")
		self.media_list_player.stop()
