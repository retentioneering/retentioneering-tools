# Path Patterns

A path pattern says "this happened, then that". It is the shared language behind
[Step Matrix](/docs/widgets/step-matrix)'s `path_pattern`, the
[`matches_pattern`](/docs/path-metrics) metric, and the `start_anchor`/`end_anchor`
anchors of [`truncate_paths`](/docs/data-processors/truncate-paths). One parser,
one set of rules, one set of error messages — whatever accepts a pattern accepts
all of the syntax on this page.

The design borrows Python's regular expressions with one substitution: **a token
is an event, not a character.** If you know regex, the only things you have to
unlearn are on this page, and there are two of them.

## The tokens

| Token | Matches | Example |
|---|---|---|
| `cart` | that event | `add_to_cart->cart` |
| `[a\|b\|c]` | one event, any of those listed | `[search\|filter_results]->add_to_cart` |
| `[^a]` | one event, anything but `a` | `add_to_cart->[^cart]` |
| `[^a\|b]` | one event, none of those listed | `catalog->[^cart\|purchase]` |
| `.` | one event, any of them | `.->product_view` |
| `.*` | any run of events, including none | `catalog->.*->purchase` |
| `[^a]*` | a run containing no `a` | `add_to_cart->[^support_chat]*->purchase` |
| `[^a\|b]*` | a run containing none of those listed | `add_to_cart->[^support_chat\|error_page]*->purchase` |
| `[a\|b]*` | a run containing nothing but those listed | `cart->[shipping_details\|payment_details]*->purchase` |
| `path_start`, `path_end` | a path's own boundaries | `path_start->home` |

Tokens are separated by `->`. Tokens not separated by `.*` must be **strictly
adjacent** in the path: `add_to_cart->cart` means cart came immediately next,
while `add_to_cart->.*->cart` means it came eventually. A pattern matches
anywhere in a path — there is no implicit anchoring at either end.

Every token except a gap occupies exactly one **position**. That matters for
`at=` in an anchor spec, for `occurrence=`, and for Step Matrix centring, all of
which count positions:

```
[search|catalog] -> .* -> add_to_cart -> [^cart]
    position 0      —      position 1     position 2
```

A class is one position no matter how many events it lists. A gap is not a
position at all — quantifying a class with `*` turns it from "one event like
this" into "a run of events like this", and runs take no ordinal.

## Two things regex users have to unlearn

**`^` and `$` are not anchors.** A path already has names for its own boundaries
— `path_start` and `path_end` are real events that appear in patterns like any
other token, and the rest of the library uses them everywhere. So negation is
written *inside* the brackets, exactly where regex puts it in a character class:
`[^cart]`, never `^[cart]`. Writing `^cart` or `cart$` is an error that points
you at the sentinels.

**Members are separated by `|`.** A regex character class is written by
juxtaposition (`[ab]`) because a character is one symbol wide. Event names are
not, so `[add_to_cart|cart]` needs the separator. Inside brackets `|` means "any
of these"; it does not work outside them, because a pattern cannot branch into
alternative sequences.

## Boundaries are not events

`.` and `[^...]` never match `path_start` or `path_end`, mirroring regex, where
`.` does not match a string boundary because a boundary is not a character. A
sentinel takes part only when you name it: `[path_end|error_page]` matches both,
`[^cart]` matches neither.

This is what makes the following work:

```python
# a product_view that was NOT the first event of the path
stream.step_matrix(path_pattern=".->product_view")
```

`.` requires a real event before the `product_view`, and the `path_start` row
does not qualify. Without the rule, `catalog->[^cart]` would be true for any
path *ending* in `catalog` — the `path_end` row is, after all, "not cart" — which
is the opposite of what the pattern says.

The rule falls out of the definition rather than being special-cased, so
`[^path_start]` is legal and means exactly the same as `.`.

## Restricting what lies between

Quantify a class with `*` and it stops being a position: it becomes a **gap**
that says what may appear in the run between the two anchors around it.

```python
# bought without ever contacting support
stream.filter_paths({"op": "=", "metric": "matches_pattern", "value": True,
                     "metric_args": {"pattern": "add_to_cart->[^support_chat]*->purchase"}})

# went from cart to purchase through checkout steps only, no wandering
stream.step_matrix(path_pattern="cart->[shipping_details|payment_details]*->purchase")
```

`.*` is the unrestricted case of the same construct — `.` repeated — which is
why `[^X]*` composes so plainly. It is also why negation is written *inside*
the brackets: with `^[X]*` it would be unreadable whether the repetition sits
inside the negation or outside it.

A gap matches the empty run, so `A->[^X]*->B` is true for an adjacent `A->B`.
That is the same rule `.*` has always followed.

A negated gap is a **blacklist** and a positive one a **whitelist**, and the
choice matters as your event vocabulary grows: `[^support_chat|error_page]*`
keeps meaning what it meant when a new event appears, while
`[shipping_details|payment_details]*` silently gets stricter. Pick by which list
you can keep complete.

**A restricted gap needs an anchor on both sides.** At either end of a pattern
its outer side is unpinned, and since the empty run always satisfies it, it
would silently mean nothing:

```python
stream.step_matrix(path_pattern="[^purchase]*->cart")
# PatternSyntaxError: '[^purchase]*' at the start of '[^purchase]*->cart' has
# nothing on its outer side to bound it, so it would match the empty run and
# mean nothing. Anchor it — e.g. 'path_start->[^purchase]*->cart'.
```

Naming `path_start` or `path_end` is usually what was meant:
`path_start->[^purchase]*->path_end` selects paths that never purchased.

## Examples

**One position, negated or alternated:**

| Pattern | Question it answers |
|---|---|
| `path_start->[^home]` | paths that did not start on the home page |
| `.->product_view` | product views that were not the path's first event |
| `add_to_cart->[^cart]` | added to cart, then went somewhere other than the cart |
| `[search\|filter_results\|compare]->add_to_cart` | added to cart straight after any research action |
| `[payment_error\|checkout_bug]->path_end` | paths that ended right after a checkout failure |
| `catalog->.->purchase` | exactly one event between the catalog and the purchase |

**A whole run, restricted:**

| Pattern | Question it answers |
|---|---|
| `add_to_cart->[^support_chat]*->purchase` | bought without contacting support |
| `add_to_cart->[^support_chat\|error_page]*->purchase` | bought without hitting either support or an error |
| `cart->[shipping_details\|payment_details]*->purchase` | checked out without wandering off |
| `path_start->[^purchase]*->path_end` | never purchased |
| `home->[^error_page]*->purchase` | converted along a clean path |

**Combined with anchors and centring:**

```python
# centre a Step Matrix on either failure, without merging the two events first
stream.step_matrix(path_pattern="[payment_error|checkout_bug]")

# a window opening at the first search-or-catalog that leads to a purchase
stream.truncate_paths(
    start_anchor={"pattern": "[search|catalog]->.*->purchase", "at": 0},
    end_anchor="purchase",
)

# a per-path 0/1 metric
stream.get_metrics([
    {"metric": "matches_pattern",
     "metric_args": {"pattern": "promo_page->.*->[purchase|cart]"}},
])
```

Centring on a class behaves exactly as centring on an event: the anchor resolves
to a *position*, and a class fills one. The single visible difference is that
column `0` of the resulting matrix holds a distribution over the class members
instead of one event at `1.0`.

**The workaround this replaces.** Before classes existed, "any of these events"
meant merging them with `rename_events` — damaging the stream to ask one
question. `[a|b]` gives the same answer with the data left alone.

## What is not supported

Negation and alternation work **exactly where their scope is bounded** — one
position, or a run pinned by an anchor on each side. Anything wider is rejected
by the parser rather than half-supported:

| Rejected | Why |
|---|---|
| `[^cart->purchase]` | a class describes one event, not a sequence |
| `cart->purchase\|catalog->add_to_cart` | a pattern cannot list alternative sequences |
| `(cart\|purchase)` | round brackets are not part of the syntax; write `[cart\|purchase]` |
| `^cart`, `cart$` | not anchors — write `path_start->cart`, `cart->path_end` |
| `[cart\|purchase]+`, `[^cart]?` | only `*` is supported after a class |
| `[^cart]*->purchase` | a restricted gap at either end has nothing bounding its outer side |
| `cart->.*->[^purchase]*->catalog` | two gaps in a row |
| `[]`, `[^]`, `[^[a\|b]]` | empty or nested classes |

Negating a whole sequence has no left boundary: the complement of an unanchored
sequence matches almost every path, and computing it needs a different kind of
engine than the relational one underneath. A restricted gap escapes this
precisely because both of its ends are pinned. To *exclude paths* containing a
sequence, use a [Filter Paths](/docs/data-processors/filter-paths) condition with
`{"op": "NOT", "args": [...]}` instead — that is a statement about whole paths,
which is a different question from what a pattern position asks.

## Mistakes the parser catches

Every name in a pattern is checked against the events actually present in the
eventstream, **including names inside a class**. This matters more for a class
than for a bare token: a typo in `purchse` matches nothing and gives you an empty
result, but a typo in `[^purchse]` excludes nothing and gives you an
*always-true* position — a wrong answer that looks like a healthy one.

```python
stream.step_matrix(path_pattern="[^Purchse]->cart")
# InvalidParameterError: Invalid value 'Purchse' for parameter 'path_pattern'.
# Allowed values: ['account_page', 'add_to_cart', ..., 'purchase', ...]
```

Two more:

- **A pattern with nothing to look for** — every position negated or a wildcard,
  like `[^cart]->.*->.` — is legal but almost always true, and warns. A positive
  class counts as something to look for: `[cart|purchase]` is as specific as
  `cart`, just about two events.
- **An event named like a class.** A token is a class only when it is *entirely*
  bracketed, so an event called `checkout [beta]` is unaffected. Only an event
  named exactly `[checkout]`, or one containing `|` that lands inside brackets,
  collides — and rather than inventing an escape syntax for something this rare,
  the pattern is rejected with an explanation. Rename the event with
  [`rename_events`](/docs/data-processors/rename-events) to use it in a pattern.

## Which match, when there are several

A pattern usually matches a path in more than one way, and `occurrence` picks
between them. It is easiest to reason about by its result: `"first"` puts every
token as **early** as that token can be in *any valid match*, `"last"` as **late**
as it can be.

The word "valid" is load-bearing. `"last"` is not "the last occurrence of the
event" — an occurrence taking part in no complete match is not a candidate at
all. On `catalog, cart, purchase, cart`, the `cart` token of
`catalog->.*->cart->.*->purchase` anchors on the **second** event, not the
fourth, because no purchase follows the fourth.

Classes change nothing here. `[a|b]` is a token like any other, and the same
definition decides which of its matches gets the anchor.

A restricted gap does not change the definition either, but it does change which
occurrences qualify — and not always in the direction you would guess. On the
path `A, X, A, D`:

| Pattern | `occurrence="first"` anchors `A` at |
|---|---|
| `A->.*->D` | the **first** `A` — both reach `D` |
| `A->[^X]*->D` | the **second** `A` — the first one's run to `D` crosses the `X` |

Nothing special is happening: "as early as it can be **in any valid match**" is
the rule in both rows, and under the restricted gap the first `A` takes part in
no valid match at all.

### `occurrence="all"`

`"all"` picks nothing — it returns *every* position each token can occupy in
some valid match. On `A, B, A, B` the `A` token of `A->.*->B` resolves to both
`A`s, where `"first"` would give one and `"last"` the other.

Because it returns several positions per path, the rows no longer describe one
match, and anything needing a single position rejects it. `truncate_paths` does:
a window bound has to be one place to cut. `add_events` is what it is for.

## Turning a position into an event

A pattern can describe a position that no event name can — "the cart that
checkout actually followed, not the one abandoned for more browsing". But only
an *event name* can be centred on by [Step Matrix](/docs/widgets/step-matrix),
counted by a funnel, or filtered on. `add_events(anchor=...)` bridges the two:
it inserts a named event at the position a pattern resolves to.

```python
# the cart that led to checkout with no cart in between
stream = stream.add_events(
    "checkout_cart",
    anchor={"pattern": "cart->[^cart]*->shipping_details", "at": "start"},
)

# now the position is addressable like any other event
stream.step_matrix(path_pattern="checkout_cart")
stream.get_conversion_rate(start_anchor="checkout_cart", end_anchor="purchase")
```

The anchor takes the same spec as `truncate_paths`'s `start_anchor` — `pattern`,
`at`, `occurrence`, `offset`, `offset_side` — with one anchor per call rather
than a list. By default it marks one position per path; `occurrence="all"` marks
every one, which is how you count *attempts* rather than users:

```python
stream.add_events(
    "checkout_attempt",
    anchor={"pattern": "cart->[^cart]*->shipping_details",
            "at": "start", "occurrence": "all"},
)
```

The new event shares its anchor's timestamp and sorts immediately **before** it,
so a matrix centred on the marker shows the anchor event itself at column `1`
and the true run-up in the negative columns. An `offset` moves the marker off
the match — `{"pattern": "purchase", "offset": -3}` marks three events before
each purchase — and clamps to the path's own boundary rather than falling off
it. A time offset (`"30m"`) lands between events and rounds to a real one:
forward for a positive offset, backward for a negative one, unless
`offset_side` says otherwise.

### Why not just centre on the pattern?

`step_matrix(path_pattern=...)` centres each block on the *prefix* of the
pattern up to that block, so the block for `cart` in
`cart->[^cart]*->shipping_details` is centred by `path_start->.*->cart` — the
**first** cart of the path, not the one the restricted gap picked out. The
pattern still selects which paths are drawn, but the suffix cannot move the
anchor. A marker event sidesteps this: the position is resolved once, by the
full pattern, and then addressed by name.
