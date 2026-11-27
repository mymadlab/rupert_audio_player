# pylint: disable=missing-module-docstring,missing-function-docstring,not-callable
# pylint: disable=cell-var-from-loop,import-error,broad-exception-caught,duplicate-code

import asyncio
import json
import os
import re
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import TypedDict
from unittest.mock import MagicMock, patch

import vlc
from confluent_kafka import Consumer, TopicPartition
from behave import given, then, when
from rupert_prosumer import RupertProsumerAdminClient
from rupert_audio_player import RupertAudioPlayer, RupertAudioProsumer  # pylint: disable=no-name-in-module


TRACKS_DIR = Path(__file__).resolve().parent / "tracks"
AFKA_BOOTSTRAP_SERVERS_ENV = 'AFKA_BOOTSTRAP_SERVERS_ENV'


class ListenerOutcome(TypedDict):
	"""Structured terminal result from a real Kafka listener thread."""
	terminal_exception_type: str
	consecutive_error_count: int


def _kafka_bootstrap_servers() -> str:
	return os.getenv(AFKA_BOOTSTRAP_SERVERS_ENV) or 'localhost:9092'


def _use_real_kafka() -> bool:
	return bool((os.getenv(AFKA_BOOTSTRAP_SERVERS_ENV) or '').strip())


def _track_path(filename):
	return str((TRACKS_DIR / filename).resolve())


def _setup_audio_player(context):
	context.audio_player = RupertAudioPlayer(
		options="--no-video", media_policy={"allowed_media_roots": [str(TRACKS_DIR)]}
	)


def _cleanup(context):
	if hasattr(context, "audio_player") and context.audio_player.media_list_player:
		context.audio_player.set({"play": "stop"})


def _start_playing_track(context, filename):
	context.current_track_name = filename
	context.current_track_path = _track_path(filename)
	context.audio_player.set(
		{
			"play_tracks": [context.current_track_path],
			"play_track": context.current_track_path,
			"play": "play",
		}
	)


def _media_mrl_contains(context, name):
	return context.audio_player.media and name in context.audio_player.media.get_mrl()


@given("an audio player is set up")
def an_audio_player_is_set_up(context):
	_setup_audio_player(context)


@when("we play an audio file")
def we_play_an_audio_file(context):
	_start_playing_track(context, "sine-test1.mp3")


@then("the audio should start playing")
def the_audio_should_start_playing(context):
	assert context.audio_player.media_list_player is not None
	assert context.audio_player.media_list_player.get_state() == vlc.State.Playing
	assert _media_mrl_contains(context, context.current_track_name)
	_cleanup(context)


@when("we enable loop mode for a file")
def we_enable_loop_mode_for_a_file(context):
	_start_playing_track(context, "whitenoise-test2.mp3")
	context.audio_player.set({"loop": "loop"})


@then("the audio should play that file continuously until loop mode is disabled")
def the_audio_should_play_that_file_continuously_until_loop_mode_is_disabled(context):
	assert context.audio_player.media_list_player is not None
	assert context.audio_player.media_list_player.get_state() == vlc.State.Playing
	assert _media_mrl_contains(context, context.current_track_name)
	_cleanup(context)


@given("an audio player is playing a file")
def an_audio_player_is_playing_a_file(context):
	_setup_audio_player(context)
	_start_playing_track(context, "silence-test3.mp3")


@when("we pause the playback")
def we_pause_the_playback(context):
	context.audio_player.set({"play": "pause"})


@then("the audio should pause")
def the_audio_should_pause(context):
	assert context.audio_player.media_list_player is not None
	assert context.audio_player.media_list_player.get_state() == vlc.State.Paused


@when("we resume the playback")
def we_resume_the_playback(context):
	context.audio_player.set({"play": "play"})


@then("the audio should continue playing")
def the_audio_should_continue_playing(context):
	assert context.audio_player.media_list_player is not None
	assert context.audio_player.media_list_player.get_state() == vlc.State.Playing
	_cleanup(context)


@when("we stop the playback")
def we_stop_the_playback(context):
	context.audio_player.set({"play": "stop"})


@then("the audio should stop and reset to the beginning")
def the_audio_should_stop_and_reset_to_the_beginning(context):
	assert context.audio_player.media_list_player is not None
	assert context.audio_player.media_list_player.get_state() == vlc.State.Stopped
	assert _media_mrl_contains(context, context.current_track_name)
	_cleanup(context)


@when("we adjust the volume to a specific level")
def we_adjust_the_volume_to_a_specific_level(context):
	context.target_volume = 25
	context.current_track_name = "sine-test1.mp3"
	context.current_track_path = _track_path(context.current_track_name)
	context.audio_player.set(
		{
			"play_tracks": [context.current_track_path],
			"play_track": context.current_track_path,
			"volume": context.target_volume,
		}
	)


@then("the audio player should set the volume to that level")
def the_audio_player_should_set_the_volume_to_that_level(context):
	assert context.audio_player.media_list_player is not None
	volume = context.audio_player.media_list_player.get_media_player().audio_get_volume()
	assert volume == context.target_volume
	_cleanup(context)


@when("we try to play a non-existent audio file")
def we_try_to_play_a_non_existent_audio_file(context):
	context.missing_track = "missing-track.mp3"
	context.missing_track_path = _track_path(context.missing_track)
	context.play_error = None
	try:
		context.audio_player.set(
			{
				"play_tracks": [context.missing_track_path],
				"play_track": context.missing_track_path,
				"play": "play",
			}
		)
	except Exception as exc:  # pragma: no cover
		context.play_error = exc


@then("the audio player should log an error message indicating the file was not found")
def the_audio_player_should_log_an_error_message_indicating_the_file_was_not_found(context):
	assert isinstance(context.play_error, ValueError)
	assert context.audio_player.media_list_player is None
	_cleanup(context)


def _test_player(policy=None):
	instance = MagicMock()
	instance.media_list_new.return_value.add_media.return_value = 0
	media_player = instance.media_list_player_new.return_value.get_media_player.return_value
	media_player.audio_set_volume.return_value = 0
	with patch('rupert_audio_player.vlc.Instance', return_value=instance):
		player = RupertAudioPlayer(log=MagicMock(), media_policy=policy)
	return player, instance


def _test_message(payload):
	message = MagicMock()
	message.error.return_value = None
	message.value.return_value = json.dumps(payload).encode()
	message.topic.return_value = 'source-topic'
	message.partition.return_value = 0
	message.offset.return_value = 0
	message.headers.return_value = []
	message.key.return_value = None
	return message


def _real_dead_letter_run(
	directory: str,
	payload: bytes,
) -> tuple[object | None, list[ListenerOutcome]]:
	# pylint: disable=too-many-locals,too-many-branches,too-many-statements
	"""Send and process one record through a real broker, then read its DLQ record."""
	if not _use_real_kafka():
		raise RuntimeError("Real Kafka mode is not configured")
	suffix = uuid.uuid4().hex
	source = f'rupert.audio.source.{suffix}'
	dlq = f'rupert.audio.dlq.{suffix}'
	path = Path(directory) / 'real-config.json'
	settings = {
		'kafka': {
			'connection': {
				'bootstrap.servers': _kafka_bootstrap_servers(),
				'message.timeout.ms': 15000,
			},
			'consumer': {
				'group.id': f'rupert-audio-behave-{suffix}',
				'auto.offset.reset': 'earliest',
				'enable.auto.commit': False,
				'enable.auto.offset.store': False,
			},
			'topics': {'source': source, 'dlq': dlq},
			'dead_letter_topic': 'dlq',
		},
		'logging': {'log_directory': directory, 'rotation': None, 'retention': None},
	}
	path.write_text(json.dumps(settings), encoding='utf-8')

	vlc_instance = MagicMock()
	vlc_instance.media_list_new.return_value.add_media.return_value = 0
	with patch('rupert_audio_player.vlc.Instance', return_value=vlc_instance):
		player = RupertAudioPlayer(
			log=MagicMock(),
			media_policy={'allowed_media_roots': ['/tmp']},
		)
	client = RupertAudioProsumer(str(path))
	client.rap = player
	client.consecutive_errors['max'] = 1
	admin = RupertProsumerAdminClient(str(path))
	probe = None
	listener = None
	try:
		admin.initialize()
		asyncio.run(client.send('source', payload))
		errors = []
		listener = threading.Thread(
			target=lambda: _capture_listener_error(client, 'source', errors), daemon=True
		)
		listener.start()
		_wait_for_real_listener(client, listener, 45)

		if len(errors) > 1:
			raise RuntimeError(f'Real Kafka listener failed unexpectedly: {errors}')
		if len(errors) == 1:
			outcome = errors[0]
			if not isinstance(outcome, dict):
				raise RuntimeError(f'Real Kafka listener failed unexpectedly: {outcome}')
			if outcome.get('terminal_exception_type') != 'RuntimeError':
				raise RuntimeError(f'Real Kafka listener failed unexpectedly: {outcome}')
			if outcome.get('consecutive_error_count') != 1:
				raise RuntimeError(f'Real Kafka listener failed unexpectedly: {outcome}')

		offset_probe = Consumer({
			'bootstrap.servers': _kafka_bootstrap_servers(),
			'group.id': settings['kafka']['consumer']['group.id'],
			'enable.auto.commit': False,
		})
		try:
			offsets = offset_probe.committed([TopicPartition(source, 0)], timeout=10)
			if not offsets:
				raise RuntimeError('Real Kafka returned no committed-offset result')
			if len(offsets) != 1:
				raise RuntimeError(f'Real Kafka returned {len(offsets)} committed-offset results')
			if offsets[0].error is not None:
				raise RuntimeError(f'Real Kafka source offset error: {offsets[0].error}')
			if offsets[0].offset != 1:
				raise RuntimeError(f'Real Kafka source offset was {offsets[0].offset}, expected 1')
		finally:
			offset_probe.close()

		probe = Consumer({
			'bootstrap.servers': _kafka_bootstrap_servers(),
			'group.id': f'rupert-audio-dlq-{suffix}',
			'auto.offset.reset': 'earliest',
			'enable.auto.commit': False,
		})
		probe.subscribe([dlq])
		deadline = time.monotonic() + 30
		record = None
		while time.monotonic() < deadline:
			record = probe.poll(1)
			if record is not None and record.error() is None:
				break
			record = None
		if record is None:
			raise RuntimeError('Real Kafka dead-letter record was not received')
		if ('rupert.source.topic', source.encode()) not in record.headers():
			raise RuntimeError('Real Kafka DLQ record missing source topic header')
		if ('rupert.source.offset', b'0') not in record.headers():
			raise RuntimeError('Real Kafka DLQ record missing source offset header')
		return record, errors
	finally:
		client.stop()
		if listener is not None:
			listener.join(5)
		if listener is not None and listener.is_alive():
			admin.close()
		else:
			_cleanup_real_topics(admin, source, dlq)
		if probe is not None:
			probe.close()
		client.close()


def _capture_listener_error(
	client: RupertAudioProsumer,
	topic: str,
	outcomes: list[ListenerOutcome],
) -> None:
	try:
		client.listen(topic)
	except BaseException as error:
		outcomes.append({
			'terminal_exception_type': type(error).__name__,
			'consecutive_error_count': client.consecutive_errors['count'],
		})


def _wait_for_real_listener(
	client: RupertAudioProsumer,
	listener: threading.Thread,
	timeout_seconds: float,
) -> None:
	listener.join(timeout_seconds)
	if listener.is_alive():
		client.stop()
		listener.join(5)
		if listener.is_alive():
			raise RuntimeError('Real Kafka audio listener did not stop')
		raise RuntimeError('Real Kafka audio listener timed out')


def _cleanup_real_topics(admin: RupertProsumerAdminClient, source: str, dlq: str) -> None:
	cleanup_errors = []
	try:
		future = admin.admin_client.delete_topics([source, dlq])
		for topic, result in future.items():
			try:
				result.result(timeout=10)
			except BaseException as error:
				cleanup_errors.append((topic, error))
	finally:
		admin.close()
	if cleanup_errors:
		details = '; '.join(f'{topic}: {error}' for topic, error in cleanup_errors)
		raise RuntimeError(f'Real Kafka topic cleanup failed: {details}')


@then("the production audio consumer dead-letters a failed control event")
def audio_failure_is_dead_lettered(_context):
	with tempfile.TemporaryDirectory() as directory:
		path = Path(directory) / 'config.json'
		path.write_text(json.dumps({
			'kafka': {
				'connection': {'bootstrap.servers': _kafka_bootstrap_servers()},
				'consumer': {'group.id': 'test'},
				'topics': {'source': 'source-topic'},
				'dead_letter_topic': 'dlq',
			},
			'logging': {'log_directory': directory, 'rotation': None, 'retention': None},
		}), encoding='utf-8')
		vlc_instance = MagicMock()
		vlc_instance.media_list_new.return_value.add_media.return_value = 0
		with patch('rupert_audio_player.vlc.Instance', return_value=vlc_instance):
			player = RupertAudioPlayer(
				log=MagicMock(),
				media_policy={'allowed_media_roots': ['/tmp']},
			)
		if _use_real_kafka():
			record, errors = _real_dead_letter_run(
				directory, b'{"action_type":"control","volume":101}'
			)
			assert record is not None
			assert record.value() == b'{"action_type":"control","volume":101}'
			assert errors
			return
		with patch('rupert_audio_player.RupertAudioPlayer', return_value=player):
			client = RupertAudioProsumer(str(path))
		client.consecutive_errors['max'] = 1
		message = MagicMock()
		message.error.return_value = None
		message.value.return_value = b'{"action_type":"control"}'
		message.topic.return_value = 'source-topic'
		message.partition.return_value = 0
		message.offset.return_value = 0
		message.headers.return_value = []
		message.key.return_value = None
		consumer = MagicMock()
		consumer.poll.return_value = message
		consumer.commit.return_value = [MagicMock(error=None)]
		producer = MagicMock()
		order = []
		def produce(_topic, **kwargs):
			order.append('dead-letter')
			assert kwargs['value'] == message.value()
			kwargs['on_delivery'](None, None)
		producer.produce.side_effect = produce
		producer.flush.return_value = 0
		def commit(**_kwargs):
			order.append('commit')
			return [MagicMock(error=None)]
		consumer.commit.side_effect = commit
		with patch('rupert_prosumer.rupert_prosumer.Consumer', return_value=consumer), \
			 patch('rupert_prosumer.rupert_prosumer.Producer', return_value=producer), \
			 patch('rupert_prosumer.rupert_prosumer.RupertLogger'):
			try:
				client.listen('source')
			except RuntimeError as error:
				assert 'Consecutive error count' in str(error)
			else:
				raise AssertionError('Expected processing failure')
		assert order == ['dead-letter', 'commit']
		player.set.assert_called_once()


@then("the documented audio events use the supported discriminator")
def documented_events(_context):
	readme = (Path(__file__).resolve().parents[2] / 'README.md').read_text(encoding='utf-8')
	examples = [json.loads(block) for block in re.findall(r'```json\n(.*?)\n```', readme, re.S)]
	events = [event for event in examples if 'action_type' in event]
	assert len(events) == 5
	client = RupertAudioProsumer.__new__(RupertAudioProsumer)
	client.logger = MagicMock()
	client.rap = MagicMock()
	for event in events:
		client.process_event(_test_message(event))
	assert client.rap.set.call_count == len(events)


@then("invalid and failed audio controls reach the dead-letter topic")
def failed_controls(_context):
	cases = [
		({'action_type': 'unknown'}, None),
		({'event_type': 'control'}, None),
		({'action_type': 'control', 'play': 'pause'}, 'pause'),
		({'action_type': 'control', 'play': 'stop'}, 'stop'),
		({'action_type': 'control', 'volume': 50}, 'volume'),
	]
	for payload, failure in cases:
		with tempfile.TemporaryDirectory() as directory:
			path = Path(directory) / 'config.json'
			path.write_text(json.dumps({
				'kafka': {'connection': {'bootstrap.servers': _kafka_bootstrap_servers()},
						  'consumer': {'group.id': 'test'},
						  'topics': {'source': 'source-topic'}, 'dead_letter_topic': 'dlq'},
				'logging': {'log_directory': directory, 'rotation': None, 'retention': None},
			}), encoding='utf-8')
			vlc_instance = MagicMock()
			vlc_instance.media_list_new.return_value.add_media.return_value = 0
			with patch('rupert_audio_player.vlc.Instance', return_value=vlc_instance):
				player = RupertAudioPlayer(log=MagicMock(), media_policy={})
			player.media_list_player = MagicMock()
			if failure == 'pause':
				player.media_list_player.pause.side_effect = RuntimeError('pause failed')
			elif failure == 'stop':
				player.media_list_player.stop.side_effect = RuntimeError('stop failed')
			elif failure == 'volume':
				player.media_list_player.get_media_player.return_value.audio_set_volume.return_value = -1
			if _use_real_kafka():
				record, errors = _real_dead_letter_run(
					directory, json.dumps(payload).encode()
				)
				assert record is not None
				assert record.value() == json.dumps(payload).encode()
				assert errors
				continue
			with patch('rupert_audio_player.RupertAudioPlayer', return_value=player):
				client = RupertAudioProsumer(str(path))
			client.consecutive_errors['max'] = 1
			consumer = MagicMock()
			consumer.poll.return_value = _test_message(payload)
			order = []
			consumer.commit.side_effect = lambda **_kwargs: (
				order.append('commit') or [MagicMock(error=None)]
			)
			producer = MagicMock()
			def produce(_topic, **kwargs):
				order.append('dlq')
				kwargs['on_delivery'](None, None)
			producer.produce.side_effect = produce
			producer.flush.return_value = 0
			with patch('rupert_prosumer.rupert_prosumer.Consumer', return_value=consumer), \
				 patch('rupert_prosumer.rupert_prosumer.Producer', return_value=producer), \
				 patch('rupert_prosumer.rupert_prosumer.RupertLogger'):
				try:
					client.listen('source')
				except RuntimeError as error:
					assert 'Consecutive error count' in str(error)
				else:
					raise AssertionError('Expected processing failure')
			assert order == ['dlq', 'commit']


@then("audio control logs do not disclose secrets")
def safe_logs(_context):
	policy = {'allowed_url_schemes': ['https'], 'allowed_url_hosts': ['example.org']}
	player, _ = _test_player(policy)
	client = RupertAudioProsumer.__new__(RupertAudioProsumer)
	client.logger = MagicMock()
	client.rap = player
	client.process_event(_test_message({
		'action_type': 'control',
		'play_tracks': ['https://example.org/audio?token=secret-payload&signature=secret-url']
	}))
	assert 'secret-payload' not in str(client.logger.mock_calls)
	assert 'secret-url' not in str(player.logger.mock_calls)
	player.bye()


@then("local paths and network URLs obey the media policy")
def media_policy(_context):
	with tempfile.TemporaryDirectory() as directory:
		root = Path(directory) / 'root'
		root.mkdir()
		track = root / 'track.mp3'
		track.touch()
		outside = Path(directory) / 'outside.mp3'
		outside.touch()
		(root / 'link.mp3').symlink_to(outside)
		policy = {'allowed_media_roots': [str(root)], 'allowed_url_schemes': ['https'],
				  'allowed_url_hosts': ['example.org']}
		player, instance = _test_player(policy)
		for allowed in (str(track), 'https://example.org/music?token=abc'):
			player.set({'play_track': allowed})
		for denied in (str(outside), str(root / 'link.mp3'), 'file:///etc/passwd',
				   'http://example.org/song', 'https://evil.example/song',
				   'https://user:pass@example.org/song', 'https://example.org:443/song'):
			before = instance.media_new.call_count
			try:
				player.set({'play_track': denied})
			except ValueError:
				pass
			else:
				raise AssertionError(f'Unexpectedly allowed media: {denied}')
			assert instance.media_new.call_count == before
		try:
			player.set({'play_directory': str(root)})
		except ValueError:
			pass
		else:
			raise AssertionError('Expected symlink escape rejection')
		player.bye()
		player, _ = _test_player()
		try:
			player.set({'play_track': str(track)})
		except ValueError:
			pass
		else:
			raise AssertionError('Expected default deny')
		player.bye()


@then("fresh-player controls return explicit initialization errors")
def fresh_player_controls(_context):
	player, _ = _test_player()
	controls = (
		{'play': 'play'}, {'play': 'pause'}, {'play': 'stop'},
		{'volume': 25}, {'loop': 'loop'}, {'navigate': 'next'},
	)
	for control in controls:
		try:
			player.set(control)
		except RuntimeError as error:
			assert str(error) == 'No VLC playlist is initialized'
		else:
			raise AssertionError(f'Expected initialization error: {control}')
	player.bye()


@then("the audio player receives the prosumer listener logger")
def listener_logger_is_reused(_context):
	client = RupertAudioProsumer.__new__(RupertAudioProsumer)
	client.logger = MagicMock()
	client.rap = MagicMock()
	client._configure_listener_logger('source')
	client.rap.set_logger.assert_called_once_with(client.logger)


@then("status events are not silently accepted")
def status_events(_context):
	client = RupertAudioProsumer.__new__(RupertAudioProsumer)
	client.logger = MagicMock()
	client.rap = MagicMock()
	client.process_event(_test_message({'action_type': 'status'}))
	assert client.rap.set.call_count == 0
	assert 'not supported' in str(client.logger.mock_calls)


@then("malformed audio events have explicit validation errors")
def malformed_events(_context):
	client = RupertAudioProsumer.__new__(RupertAudioProsumer)
	client.logger = MagicMock()
	client.rap = MagicMock()
	for payload, expected in (
		([], 'Audio event must be a JSON object'),
		({}, 'Unknown or missing audio action type'),
		({'action_type': 'unknown'}, 'Unknown or missing audio action type'),
	):
		try:
			client.process_event(_test_message(payload))
		except ValueError as error:
			assert str(error) == expected
		else:
			raise AssertionError(f'Expected validation error: {payload}')

	for raw_payload in (b'\xff', b'{'):
		message = MagicMock()
		message.value.return_value = raw_payload
		try:
			client.process_event(message)
		except ValueError as error:
			assert str(error) == 'Audio event must be valid UTF-8 JSON'
		else:
			raise AssertionError(f'Expected JSON validation error: {raw_payload}')

	for raw_payload in (None, 'not bytes', 42):
		message = MagicMock()
		message.value.return_value = raw_payload
		try:
			client.process_event(message)
		except ValueError as error:
			assert str(error) == 'Audio event payload must be bytes'
		else:
			raise AssertionError(f'Expected bytes validation error: {raw_payload}')


@then("invalid control field types are rejected before VLC mutation")
def invalid_control_fields(_context):
	player, instance = _test_player({'allowed_media_roots': ['/tmp']})
	invalid_controls = (
		({'shuffle': 'false'}, 'shuffle must be a boolean'),
		({'play_track': 123}, 'play_track must be a string'),
		({'play_tracks': 'track.mp3'}, 'play_tracks must be a non-empty list of strings'),
		({'play': 'running'}, "play must be 'play', 'pause', or 'stop'"),
		({'volume': True}, 'Volume must be an integer from 0 to 100'),
		({'loop': 'sometimes'}, "loop must be 'single', 'repeat', or 'loop'"),
		({'navigate': 'sideways'}, "navigate must be 'next' or 'previous'"),
		({'unknown': True}, 'Unknown audio control field(s): unknown'),
	)
	before = instance.media_list_new.call_count
	for control, expected in invalid_controls:
		try:
			player.set(control)
		except ValueError as error:
			assert str(error) == expected
		else:
			raise AssertionError(f'Expected control validation error: {control}')
	assert instance.media_list_new.call_count == before
	player.bye()


@then("single-track and combined audio controls are applied")
def combined_controls(_context):
	with tempfile.TemporaryDirectory() as directory:
		track = Path(directory) / 'track.mp3'
		track.touch()
		player, instance = _test_player({'allowed_media_roots': [directory]})
		player.set({'play_track': str(track)})
		instance.media_new.assert_called_once_with(str(track.resolve()))
		with patch('rupert_audio_player.time.sleep'):
			player.set({'play': 'pause', 'volume': 25})
		list_player = instance.media_list_player_new.return_value
		list_player.pause.assert_called_once()
		list_player.get_media_player.return_value.audio_set_volume.assert_called_once_with(25)
		player.bye()
