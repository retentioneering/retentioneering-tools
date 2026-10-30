# Role 08 — frontend and MCP data integrity preregistration

Date: 2026-10-30. Reviewer: independent role 08.

## Exposure and boundaries

Before this registration I read only `output/AB_Safe_Bet_Review/PROTOCOL.md` and the task assignment. I have not inspected candidate implementation, CHECKPOINT, peer reports, or other role artifacts. The task assignment identifies baseline commit 38cbe91d5b315584575b132a596890e69eb210aa and says bundled JS was copied from its earlier exact build. I will hash actual inspected Python and JS inputs. This is staged oracle-first review, not a blinded study. I will not edit the candidate.

## Independent oracle

`oracle.py` is written before inspecting implementation. It uses integer arithmetic and `fractions.Fraction`, with no package formulas. Counts are unique assigned subjects. A success belongs to a subject only when its timestamp is at or after that subject's assignment and at or before a fixed observation cutoff. An assignment after cutoff contributes no observation. Repeated success events do not add successes. Each subject has one fixed arm. Raw data are deliberately reordered.

Fixture A: control 2 of 4 and treatment 3 of 5. Expected conversion: 50% and 60%; absolute delta: +10 percentage points; relative lift: +20%. Fixture B: control 0 of 3 and treatment 1 of 3; absolute delta +33⅓ percentage points; relative lift unavailable, never infinity. Fixture C: treatment unavailable/empty; point and comparison displays must not imply evidence or show a numeric zero as an observed rate. Fixture D: fixed assignments and staggered event timestamps expose pre-assignment, after-cutoff, duplicate, and foreign-subject events; expected counts are handwritten separately below. Fixture E checks a reduction from 3/4 to 1/4: -50 percentage points and -66⅔% relative lift.

Serialization contract: structured JSON sent to users must serialize with `allow_nan=False`; unavailable values use null/explicit status. Invalid unstructured objects must fail clearly, not silently turn into plausible numeric statistics. This is a presentation expectation, not an assumption about method names.

## Checks, ordered before unblinding

1. Discover public API entry points, using source-selectable adapters; preserve the oracle unchanged.
2. Compare arm counts, successes, point estimates, deltas, relative lift and labels against oracle for each surface available: headless result, widget traits, static HTML, real browser DOM, MCP text/structured payload.
3. Check assignment/observation-cutoff provenance, warnings and unsupported inference states; no evidence-strength claim should be introduced by a renderer.
4. Check widget state restore and repeat calls for consistency; record whether a test is real browser, DOM-only, stubbed, or Python-only.
5. Check report tabs and code examples for faithful exposure of actual capabilities.
6. Use strict JSON decoding/encoding for finite handling and stable identifiers.

## Initial severity rule and reporting

P1: a UI/MCP surface presents materially wrong counts/effect/direction or falsely claims usable inference. P2: a public supported path loses warnings, binding/cutoff provenance, or state in a way that can mislead an analyst. P3: bounded display/serialization issue with clear recoverable context. Classify defects separately from estimator limits, misuse, and untested scope. Source-selected baseline runs and candidate followup will remain separate. Deliver raw evidence, runnable adapters, command log, function matrix, Markdown and HTML report.

## Handwritten cutoff oracle D

Cutoff 2026-01-03T00:00:00Z. c1 assigned Jan 1, succeeds Jan 2 => success. c2 assigned Jan 2, success Jan 1 => failure. c3 assigned Jan 1, success Jan 4 => failure. t1 assigned Jan 1, success Jan 2 twice => one success. t2 assigned Jan 2, success exactly cutoff => success. t3 assigned Jan 4 => excluded. Foreign subject u1 succeeds Jan 2 but is unassigned => excluded. Thus control 1/3, treatment 2/2, absolute delta +66⅔ percentage points, relative lift +200%.
