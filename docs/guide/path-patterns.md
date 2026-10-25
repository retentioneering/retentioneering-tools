# Path Patterns

A path pattern says "this happened, then that". It is the shared language behind
[Step Matrix](/docs/widgets/step-matrix)'s `path_pattern`, the
[`matches_pattern`](/docs/path-metrics) metric, and the `start_anchor`/`end_anchor`
anchors of [`truncate_paths`](/docs/data-processors/truncate-paths). One parser,
one set of rules, one set of error messages — whatever accepts a pattern accepts
all of the syntax on this page.

The design borrows Python's regular expressions with one substitution: **a token
is an event, not a character.** If you know regex, the things you have to unlearn
are collected in [one section](#what-regex-users-have-to-unlearn), and there are
four of them.

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
| `.{m,n}` | a run of `m` to `n` events (also `{n}`, `{m,}`, `{,n}`, `+`, `?`) | `add_to_cart->.{0,5}->purchase` |
| `(a->b){k}` | the sequence `a->b`, `k` times in a row | `catalog->.*->(cart->.*){2}` |
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
position at all — quantifying a class with `*` (or any other quantifier) turns
it from "one event like this" into "a run of events like this", and runs take no
ordinal.

## What regex users have to unlearn

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

**A quantified class is one gap, not several positions.** In regex `[ab]{2}` is
two characters. Here `[a|b]{2}` matches the same thing — two events, each `a` or
`b` — but as a single gap that takes no position, so it needs an anchor on each
side and adds nothing to `at=` or to Step Matrix's blocks. To repeat events *as
positions*, use a group: `(a){2}`. See [Bounding how long a run is](#bounding-how-long-a-run-is)
and [Square brackets or round ones](#square-brackets-or-round-ones).

**A repetition count is a lower bound.** A pattern matches anywhere in a path, so
`(cart){2}` is true of a path with three carts in a row — two of them are in a
row. Regex's `re.search` behaves the same way; what regex users usually reach for
instead is a fully anchored `re.fullmatch`, which patterns do not have. "At most"
and "exactly" are therefore written as conditions — see
[Counting repetitions](#counting-repetitions).

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

## Bounding how long a run is

`*` is one member of the regex quantifier family, and the rest of the family
works on every gap — `.` or a class in brackets:

| Quantifier | Run length | Same as |
|---|---|---|
| `*` | any, including none | `{0,}` |
| `+` | at least one | `{1,}` |
| `?` | none or one | `{0,1}` |
| `{n}` | exactly `n` | `{n,n}` |
| `{m,}` | at least `m` | |
| `{,n}` | at most `n` | `{0,n}` |
| `{m,n}` | from `m` to `n` | |

As in regex, both bounds are **inclusive**, and the anchors are **not counted**:
`A->.{0,5}->B` allows at most five events *between* `A` and `B`. Mind the off-by-one
when translating a question — "bought within five steps of the cart", if the
fifth step is the purchase itself, is `add_to_cart->.{0,4}->purchase`.

```python
# converted quickly: at most three events between the cart and the purchase
stream.get_metrics([{"metric": "matches_pattern",
                     "metric_args": {"pattern": "add_to_cart->.{,3}->purchase"}}])

# went through checkout steps only, and at least one of them
stream.step_matrix(path_pattern="cart->[shipping_details|payment_details]+->purchase")
```

The second example is the one `*` cannot say. `cart->[shipping_details|payment_details]*->purchase`
is also true of a cart followed directly by a purchase: the empty run contains
nothing forbidden. `+` rules that out.

**A bounded gap is still one gap, never several positions.** In regex `[ab]{2,3}`
is two or three characters; here it takes no ordinal at all, so `at=` and Step
Matrix centring keep addressing the same tokens whatever the run's length. The
paths matched are exactly regex's — only the numbering differs. `A->.->.->B`
and `A->.{2}->B` match the same paths, but in the first `B` is position 3 and in
the second position 1.

Matching is a search, so a bound holds for **some** pair of anchors, not for all
of them. On `A, x, x, x, A, B`, `A->.{0}->B` is true (the second `A`) and so is
`A->.{4}->B` (the first). To say that *no* pair is close, negate the pattern
with a condition — see [What is not supported](#what-is-not-supported).

A bound counts events, not time. A unit (`{30m,}`) is rejected for now.

## Repeating a sequence

Round brackets group a sequence so a quantifier can repeat it. The group is
**unrolled** into copies before matching, and the copies are joined the way any
two tokens are — back to back:

```
(cart->purchase){2}          = cart->purchase->cart->purchase
catalog->.*->(cart->.*){2}   = catalog->.*->cart->.*->cart
```

Each copy is ordinary pattern, so each takes its own positions for `at=` and
Step Matrix: `A->(B->C){2}` has five.

**Put the gap inside the group** when the repetitions may be spread along the
path — `(B->.*->C->.*){2}` is "`B`, later `C`, and that twice". Without the
trailing `.*` the copies would be glued as `C->B`. Because that is almost never
what is meant, the parser rejects it and offers both readings:

```python
stream.get_metrics([{"metric": "matches_pattern",
                     "metric_args": {"pattern": "A->.*->(B->.*->C){2}"}}])
# PatternSyntaxError: In '(B->.*->C){2}' the repetitions are joined back to
# back, so the group's last event must be followed immediately by its first
# ('C' then 'B', nothing between). For repetitions anywhere along the path
# write '(B->.*->C->.*){2}'; if back to back is what you mean, write it out:
# 'B->.*->C->B->.*->C'.
```

A group with no gap inside, like `(cart->purchase){3}`, is not affected — back to
back is the only thing it can mean.

**"At least", never "at most".** A search that finds `k` repetitions does not
care whether more follow, so `{k}` already means "at least `k`". `{k,}` and `+`
are accepted where a `.*` sits next to the repeated block (inside the group at
its edge, outside it, or at an end of the pattern), which is where they mean
the same as `{k}`. An upper bound — `{,n}`, `{m,n}`, `?` — is rejected, because
"at most `n`" is a statement about the *absence* of another repetition — see
[Counting repetitions](#counting-repetitions) for how to say it.

The rule for `{k,}` exists because between two adjacent tokens unrolling cannot
express it. `A->(B->C){2,}->D` would have to match `A,B,C,B,C,B,C,D`, which
`A->(B->C){2}->D` does not (the `D` is not right after the second `C`); with a
`.*` anywhere next to the block, the extra copies fall into it and the two
spellings agree. Rather than accept a form that silently means `{k}` in one
place and something unexpressible in another, the parser refuses it where they
differ.

Repetitions are counted the way `re.findall` counts them: one after another,
never overlapping. On `B, B, C, C` the sequence `B->.*->C` occurs **once**, not
four times — the second repetition would need a `B` after the first `C`.

A group cannot contain another group (`((A->B){2}->C){2}`) — unroll the inner one
by hand — and must repeat at least once: `(P){0}`, `(P)*` and `(P)?` would be
true of every path.

## Square brackets or round ones

Both take quantifiers, and on a single event they can even look alike — but they
answer different questions:

| | `[a\|b]` — a class | `(a->b)` — a group |
|---|---|---|
| Contains | event names separated by `\|` | any pattern, tokens separated by `->` |
| Means | **one** event, any of those listed | a **sequence** of events |
| Without a quantifier | one position | not allowed — a group needs a quantifier |
| With a quantifier | a **gap**: a run between two anchors | **copies**: the sequence unrolled `k` times |
| Positions it takes | none | every copy takes its own |
| Can stand on its own | no — needs an anchor on each side | yes |
| Upper bound (`{,n}`, `{m,n}`, `?`) | yes | no — use a condition |

On one event the difference is the one between a run and a repetition:

```python
# between X and Y, two or more events — all of them A
"X->[A]{2,}->Y"

# A twice in a row, anywhere in the path; two positions, both addressable by at=
"(A){2,}"            # same as "A->A"
```

`[A]{2,}` on its own is refused — a gap is the run *between* anchors, and here
there are none — with a message pointing at `(A){2,}`. The same holds at either
end of a longer pattern: `cart->[A]{2,}` has nothing after the run to bound it.

A class may sit inside a group, which is how "several of these, in a row, as
positions" is written: `([search|catalog]){3}` is three research events in a row,
each a position of its own.

## Counting repetitions

A pattern can say "at least `k` times". Everything else — at most, exactly, a
range, its complement — is a [Filter Paths](/docs/data-processors/filter-paths)
condition over two such patterns, using the
[shorthand leaf](/docs/path-metrics#shorthand-leaves) for `matches_pattern`:

```python
P = "A->.*->(B->.*->C->.*)"      # "after A, a B later followed by a C"

stream.filter_paths({"matches_pattern": P + "{2}"})                    # at least twice
stream.filter_paths({"op": "not", "args": [{"matches_pattern": P + "{2}"}]})   # at most once
stream.filter_paths([{"matches_pattern": P + "{2}"},                   # exactly twice
                     {"op": "not", "args": [{"matches_pattern": P + "{3}"}]}])
```

| Wanted | Condition |
|---|---|
| at least `m` | `P{m}` |
| at most `n` | `not P{n+1}` |
| exactly `k` | `P{k}` and `not P{k+1}` |
| from `m` to `n` | `P{m}` and `not P{n+1}` |
| fewer than `m` or more than `n` | `not P{m}` or `P{n+1}` |

The last row is why no quantifier could replace the condition: the complement of
a range is two ranges.

**Back to back, these count the longest run.** For repetitions glued together —
`(A){k}`, or a gap-free `(cart->purchase){k}` — "`P{m}` and not `P{n+1}`" reads
"some run reaches `m`, and no run reaches `n+1`". On a path with one run of three
`A`s and another of six, "a run of 2 to 4" is false: the six-run reaches five.
If what you mean is "some run, taken whole, has length 2 to 4", that is a
property of maximal runs, which neither patterns nor these conditions express.

**Spread out, they count non-overlapping occurrences** in order, as `re.findall`
does — see [Repeating a sequence](#repeating-a-sequence).

## Repeated events and loops

Users going in circles are one of the most common findings in an event log. The
pattern language has no back-references (regex's `(.)\1`, "the same event
again"), so "*some* event repeated" is a condition over one pattern per event:

```python
events = stream.df[stream.schema.event_col].unique()

# any event repeated back to back at least twice — a self-loop
stream.filter_paths({"op": "or", "args": [{"matches_pattern": f"({e}){{2}}"} for e in events]})

# at least three in a row
stream.filter_paths({"op": "or", "args": [{"matches_pattern": f"({e}){{3}}"} for e in events]})

# paths with no self-loop at all
stream.filter_paths({"op": "not", "args": [
    {"op": "or", "args": [{"matches_pattern": f"({e}){{2}}"} for e in events]}
]})
```

`(e){2}` and `(e){2,}` are the same pattern — two in a row is exactly what a run of
two or more contains — and both unroll to `e->e`. In an f-string the braces are
doubled: `f"({e}){{2}}"` produces `(cart){2}`.

The obvious shortcut does not work: `[a|b|c]->[a|b|c]` is "two events *from the
set* in a row", which `a->b` satisfies just as well as `a->a`.

A **return** to an event after doing something else is a different question:

| Pattern | Matches | Does not match |
|---|---|---|
| `(A){2}` | `A, A` | `A, x, A` |
| `A->.+->A` | `A, x, A` — and also `A, A, A` | `A, A` |
| `A->[^A]+->A` | `A, x, A` | `A, A`, `A, A, A` |

`A->.+->A` is true of three `A`s in a row because the middle one is "at least one
event" between the outer two. `[^A]+` rules that out by requiring the run between
to contain something other than `A`.

Self-loops are also what [Collapse Events](/docs/data-processors/collapse-events)'
`loops=True` merges, and what a [Transition Graph](/docs/widgets/transition-graph)
draws as a node's edge to itself. Filter on them first if the question is about
the users who loop, collapse them if the loops are noise.

To **mark** each loop in place, put an event at its start with
[`occurrence="non_overlapping"`](#occurrencenon_overlapping):

```python
for e in events:
    stream = stream.add_events(
        f"{e}_loop",
        anchor={"pattern": f"({e}){{3,}}", "at": "start", "occurrence": "non_overlapping"},
    )
# B, B, B, C, B, B, D, B, B, B, B, B, E
#   -> B_loop, B, B, B, C, B, B, D, B_loop, B, B, B, B, B, E
```

Each run of three or more gets exactly one marker, wherever it is in the path —
including at its very start. `"all"` would mark every `B` a run of three can
start on, and `"first"` only the first loop of the path.

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

**A run of bounded length:**

| Pattern | Question it answers |
|---|---|
| `add_to_cart->.{0,4}->purchase` | bought within five steps of adding to cart |
| `catalog->.{10,}->purchase` | bought after a long browse from the catalog |
| `cart->[shipping_details\|payment_details]+->purchase` | checked out through at least one checkout step, and nothing else |
| `product_view->.{2}->add_to_cart` | exactly two events between the view and the add |

**A sequence, repeated:**

| Pattern | Question it answers |
|---|---|
| `path_start->.*->(add_to_cart->.*->cart->.*){2}` | went from adding to cart to the cart at least twice |
| `(product_view->catalog){3}` | bounced between a product and the catalog three times running |

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
| `(cart\|purchase)` | round brackets group a *sequence* for repetition; alternatives go in square ones: `[cart\|purchase]` |
| `^cart`, `cart$` | not anchors — write `path_start->cart`, `cart->path_end` |
| `cart{2}` | a quantifier applies to `.`, a class or a group; write `(cart){2}` for two in a row, `[cart]{2}` for a run between anchors |
| `[^cart]*->purchase`, `cart->.{3,}` | a restricted or bounded gap at either end has nothing bounding its outer side |
| `cart->.*->[^purchase]*->catalog` | two gaps in a row |
| `cart->.{30m,}->purchase` | bounds count events; time bounds are not supported |
| `A->.*->(B->.*->C){2}` | repetitions glued back to back; put the gap inside: `(B->.*->C->.*){2}` |
| `(B->C){,3}`, `(B->C){2,4}`, `(B->C)?` | "at most" is a condition on the path, not a pattern; use `NOT` |
| `A->(B->C){2,}->D` | "at least" between two adjacent tokens would need counting; use `{2}` or allow a gap |
| `((A->B){2}->C){2}` | nested groups — unroll the inner one by hand |
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

Several more:

- **A quantifier on an event name.** `cart{2}` is read as an event called
  `cart{2}`, so that a real name ending in `*` or `}` keeps working; when no such
  event exists, the error suggests `(cart){2}` (two in a row) and `[cart]{2}` (a
  run between anchors).
- **Glued repetitions.** `(B->.*->C){2}` is refused with both readings, as shown in
  [Repeating a sequence](#repeating-a-sequence).
- **A bounded or restricted gap at an end.** `[A]{2,}` alone, or `cart->.{3,}`,
  has nothing on its outer side; the error names the group form and the anchored
  form (`cart->.{3,}->path_end`).
- **Malformed bounds.** `{3,1}`, `{-1}`, `{}`, `{,}` and a time unit (`{30m}`) are
  each rejected with a message naming the problem.
- **An event named like a gap.** An event called exactly `.{2}` or `[a]+` collides
  the way one called `[checkout]` does, and is rejected the same way.
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

## Pitfalls

The parser catches what it can; these are the mistakes it cannot, because each
is a valid pattern meaning something other than what was intended.

- **Off by one on "within `n` steps".** A bound counts the events *between* the
  anchors. "Purchased within five steps of the cart" is `.{0,4}` if the purchase
  is the fifth step, `.{0,5}` if five steps may come before it.
- **`*` on a whitelist is satisfied by nothing.** `cart->[ship|pay]*->purchase` is
  true of an adjacent cart and purchase. If at least one checkout step must have
  happened, write `+`.
- **A bound holds for some pair of anchors.** `A->.{0,3}->B` is true if *any* `A`
  is close to *any* later `B`. "Every purchase came quickly" is the negation of a
  pattern for a slow one — `not (A->.{4,}->B)` — and even that reads "no `A` is
  far from a later `B`".
- **A count on a group is a lower bound.** `(cart){2}` is true of three carts in a
  row. Use [Counting repetitions](#counting-repetitions) for an exact number.
- **A group shifts `at=`.** Every copy takes positions, so in `catalog->(cart){2}`
  position 1 is the first `cart` and position 2 the second — `at=-1` is the safe
  way to say "the last one".

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

A bounded gap filters occurrences the same way. On `A, x, x, A, B`:

| Pattern | `"first"` anchors `A` at | `"last"` anchors `A` at |
|---|---|---|
| `A->.*->B` | the first `A` | the second `A` |
| `A->.{0,1}->B` | the **second** `A` — the first is three events away | the second `A` |
| `A->.{2,}->B` | the first `A` | the **first** `A` — the second is adjacent to `B` |

A group adds positions but changes nothing here: its copies are ordinary tokens,
each placed as early or as late as a valid match allows.

### `occurrence="all"`

`"all"` picks nothing — it returns *every* position each token can occupy in
some valid match. On `A, B, A, B` the `A` token of `A->.*->B` resolves to both
`A`s, where `"first"` would give one and `"last"` the other.

Because it returns several positions per path, the rows no longer describe one
match, and anything needing a single position rejects it. `truncate_paths` does:
a window bound has to be one place to cut. `add_events` is what it is for.

### `occurrence="non_overlapping"`

`"all"` answers "where can this token be?", and overlapping matches all
contribute. To count or mark each *instance* of a pattern, the question is
different: take the leftmost match, resume after it, repeat. That is what
`re.finditer` does, and `"non_overlapping"` does exactly that — the pattern is
compiled to a Python regex over the path, one character per event, so leftmost,
non-overlapping and backtracking behave as in regex.

On `B, B, B, B, B` with `(B){3,}`:

| `occurrence` | `at="start"` lands on | Why |
|---|---|---|
| `"first"` | step 1 | one position per path |
| `"all"` | steps 1, 2, 3 | a run of three can start on any of them |
| `"non_overlapping"` | step 1 | one match, which takes the whole run |

Two rules turn the regex translation into what an analyst expects:

- **A repeated group is greedy.** `(B){3,}` takes the whole run, so `B×5` is one
  match. A closed count is not: `(B){3}` on `B×6` is two matches of three. The
  group's first copy is the run's first repetition and its other copies the
  run's last ones — `at="start"` is where the run begins, `at="end"` where it
  ends. With `+` there is only one copy, the first repetition, so to address a
  run's end use `{2,}`.
- **A gap is lazy** (regex `.*?`). `A->.*->B` pairs each `A` with the *next*
  `B`: on `A, x, B, A, B` it finds two matches. A greedy gap would find one,
  from the first `A` to the last `B`. Matches still do not overlap, so on
  `A, A, B` there is one match — it starts on the first `A`, and the second is
  inside it.

Like `"all"`, it returns several positions per path, so `truncate_paths`, Step
Matrix and Step Sankey reject it; `add_events` is what it is for, and
`get_conversion_rate` counts each path once however many matches it has.

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

### When a pattern is enough on its own

`step_matrix(path_pattern=...)` already centres on the position the pattern
names — every block anchors on the same whole-pattern match, so
`path_pattern="cart->[^cart]*->shipping_details"` gives one block centred on
that cart and one on the checkout. If a centred matrix is all you need, the
pattern alone does it, and a marker event adds nothing.

A marker earns its place when the position has to be **addressed elsewhere**:
counted by a funnel, filtered on, used as a conversion anchor, compared across
segments. It also reaches two things a pattern cannot say — `occurrence`, where
the structural spelling gets unwieldy (`cart->[^cart]*->pay->[^cart]*->path_end`
for "the last checkout attempt"), and `offset`, which has no pattern form at all
for time (`{"offset": "30m"}`).
