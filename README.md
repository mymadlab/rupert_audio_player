# rupert-audio-player

`rupert-audio-player` is a Kafka-driven audio playback service for Rupert.
It consumes JSON control events, manages playback through VLC, and supports single track, playlist, shuffle, repeat, directory-based playlists, and transport controls.

## Service Notes

- Service entrypoint class: `RupertAudioProsumer` in `rupert_audio_player.py`.
- Player engine class: `RupertAudioPlayer` in `rupert_audio_player.py`.
- Event handling:
  - `action_type == "control"`: applies playback changes via `RupertAudioPlayer.set(...)`.
  - `action_type == "status"`: explicitly rejected because status publishing is not implemented.
  - Other action types are rejected. `event_type` is not accepted as a substitute.
- Supported controls in the event payload:
  - `play`: `"play" | "pause" | "stop"`
  - `play_track`: single track path
  - `play_tracks`: list of track paths
  - `play_directory`: directory containing audio files (auto-filtered)
  - `shuffle`: `true | false`
  - `loop`: `"single" | "repeat" | "loop"`
  - `navigate`: `"next" | "previous"`
  - `volume`: integer 0-100 (VLC scale)

## Example JSON Events

### 1) Play a Single Track

```json
{
  "action_type": "control",
  "play_tracks": ["/data/audio/sine-test1.mp3"],
  "play": "play"
}
```

### 2) Play a Playlist with Shuffle

```json
{
  "action_type": "control",
  "play_tracks": [
    "/data/audio/track-a.mp3",
    "/data/audio/track-b.mp3",
    "/data/audio/track-c.mp3"
  ],
  "shuffle": true,
  "play": "play"
}
```

### 3) Add a Directory as Playlist

```json
{
  "action_type": "control",
  "play_directory": "/data/audio",
  "play": "play"
}
```

### 4) Playback Navigation and Repeat

```json
{
  "action_type": "control",
  "navigate": "next",
  "loop": "loop"
}
```

### 5) Pause / Resume / Stop / Volume

```json
{
  "action_type": "control",
  "play": "pause",
  "volume": 25
}
```

## Configuration Example

This service extends `RupertProsumer`, so it expects the same Kafka and logging configuration structure used by `rupert-prosumer`.
Audio control errors propagate to the prosumer for dead-letter delivery; the
configured dead-letter topic must exist before listening.

```json
{
  "kafka": {
    "connection": {
      "bootstrap.servers": "localhost:9092"
    },
    "consumer": {
      "group.id": "rupert-audio-player-group",
      "auto.offset.reset": "earliest"
    },
    "dead_letter_topic": "dead_letter",
    "topics": {
      "audio_control": "rupert.audio.control",
      "dead_letter": "dead.letter.queue"
    }
  },
  "logging": {
    "rotation": "10 MB",
    "retention": "7 days",
    "level": "INFO"
  },
  "audio": {
    "allowed_media_roots": ["/data/audio"],
    "allowed_url_schemes": [],
    "allowed_url_hosts": []
  }
}
```

### Media access and trust

Media is denied by default. Configure `audio.allowed_media_roots` with directories
containing trusted local audio files. Resolved files and directories must remain
inside a configured root; symlinks to locations outside it are rejected. The
roots and their parent directories must not be writable by untrusted users:
checks occur before VLC opens the file and cannot prevent concurrent replacement.
Local media formats and files must also be trusted, since VLC can interpret
playlist contents and access references within them. Configure this policy in
the service's JSON configuration before deployment; restart the audio service
after changing it.

To allow network streams, explicitly configure both `allowed_url_schemes`
(`http` and/or `https`) and exact lowercase `allowed_url_hosts` (without ports).
URLs with userinfo or explicit ports are rejected. This validates only the
initial URL: VLC may follow redirects, resolve DNS again, or open playlist
references. Enforce all actual network destinations with external egress
controls, and trust content served by allowed hosts. Do not rely on this
allowlist alone as an SSRF boundary.

Playback logs do not include event payloads or media identifiers. `status()`
returns media identifiers to its caller, which must protect them. The service
releases VLC resources on normal exit; callers using the player directly should
call `bye()` when finished. `play` requests initiate asynchronous VLC playback;
a later decoder or network failure cannot be reported as a processing failure
by the Kafka handler.

## Python Module Requirements

The repository provides exact package constraints for the tested Python 3.12 environment in `constraints.txt`.

- Python: `3.12.x`
- `python-vlc`: `3.0.21203`
- `beartype`: `0.22.9`
- `loguru`: `0.7.3`
- `confluent-kafka`: `2.14.0`
- `behave`: `1.3.3` (for BDD tests)
- `rupert-prosumer`: install alongside this service from the Rupert prosumer workspace/package

Example install:

```bash
pip install -c constraints.txt "python-vlc" "beartype" "loguru" "confluent-kafka" "behave"
pip install -e ../rupert_prosumer
```

Regenerate `constraints.txt` only after testing the complete service and all required VLC system dependencies.

## VLC Software Requirement

`python-vlc` is only a Python binding. You must also install the VLC application/runtime on the host.

- macOS (Homebrew):

```bash
brew install --cask vlc
```

- Ubuntu/Debian:

```bash
sudo apt-get update && sudo apt-get install -y vlc
```

After installation, verify VLC is available to the runtime environment and that audio output is configured on the host/container.

## Running BDD Tests

Mock mode, used when no broker address is configured:

```bash
PYTHONPATH=../rupert-prosumer python -m behave features/playlist.feature
```

Real Kafka mode for the Kafka consumer scenarios:

```bash
export AFKA_BOOTSTRAP_SERVERS_ENV="broker-host:9092"
PYTHONPATH=../rupert-prosumer python -m behave features/audio_prosumer.feature
```
