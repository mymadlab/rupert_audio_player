Feature: Audio consumer failure handling

	Scenario: Audio control failures are dead-lettered
		Then the production audio consumer dead-letters a failed control event

	Scenario: Documented control events reach the player
		Then the documented audio events use the supported discriminator

	Scenario: Failed controls are dead-lettered before commit
		Then invalid and failed audio controls reach the dead-letter topic

	Scenario: Audio payloads and media identifiers stay out of logs
		Then audio control logs do not disclose secrets

	Scenario: Media access is restricted to configured locations
		Then local paths and network URLs obey the media policy

	Scenario: Player releases VLC resources on replacement and shutdown
		Then VLC resources are released including on partial initialization

	Scenario: Fresh-player controls fail with a clear initialization error
		Then fresh-player controls return explicit initialization errors

	Scenario: Status events do not produce a silent no-op
		Then status events are not silently accepted

	Scenario: Malformed events produce explicit validation errors
		Then malformed audio events have explicit validation errors

	Scenario: Invalid control fields are rejected before VLC mutation
		Then invalid control field types are rejected before VLC mutation

	Scenario: Single tracks and combined playback and volume controls work
		Then single-track and combined audio controls are applied
