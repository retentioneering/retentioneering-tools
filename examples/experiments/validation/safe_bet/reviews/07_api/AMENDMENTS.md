# Post-unblinding adapters and follow-up probes

The preregistered oracle is unchanged. The main adapter uses `diff=('group','test','control')`, because the public contract is group 1 minus group 2. Numeric data are finite in the independent oracle; documented null behavior is tested separately.

The guide explicitly states that relative effects are NaN when group 2 is zero **or negative**. This differs from the preregistered mathematical convention for a negative base but is accepted as a documented restriction, not a defect.

Initial mixed-ID metric probe never reached metric construction because its equivalent instants had mixed UTC offsets. The follow-up holds all timestamps in UTC and preserves the original IDs/values; the original failure and oracle are retained. This separates a clock-format rejection from ID handling.

Initial timezone probe used a naive `observation_end` on a timezone-aware stream. Source validation requires the cutoff to match the stream timezone, while naive `assigned_at` values are explicitly localized. The follow-up uses an aware cutoff and leaves `assigned_at` naive. The initial failure is not classified as a defect.

Additional post-source probes isolate mixed-label output, recipe state sharing/JSON serialization, typed-ID SQL coercion, timestamp checks and seed boundaries. These are adversarial software checks discovered after unblinding, not additional preregistered independent oracle outcomes.
