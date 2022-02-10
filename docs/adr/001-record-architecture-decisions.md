
# 1. Record architecture decisions

Date: 2022-02-06

## Status

Accepted

## Context

All major architectural decisions made on this project shall be record.

## Decision

We will use Architecture Decision Records (ADRs), as described by Michael Nygard
in this article:
http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions

A tool can be used to automate the process of create ADRs, such as:

* https://github.com/npryce/adr-tools
* https://github.com/marouni/adr

## Consequences

ADRs will be numbered sequentially and monotonically. Numbers will not be
reused. If a decision is reversed, we will keep the old one around, but mark it
as superseded. (It's still relevant to know that it was the decision, but is no
longer the decision.)

We will keep ADRs in the project repository under docs/adr/NNN-title.md, and
they will be written in Markdown.

A simple format with be used, so each document is easy to digest. The format has
just a few parts:

* **Title:** These documents have names that are short noun phrases. For
  example, "1. Deployment on Ruby on Rails 3.0.10" or "9. LDAP for Multi-tenant
  Integration"

* **Date:** The date that the ADR was created.

* **Status:** A decision may be "Draft" if the ADR is completed yet, "Proposed"
  if the project stakeholders haven't agreed with it yet, or "Accepted" once it
  is agreed. If a later ADR changes or reverses a decision, it may be marked as
  "Deprecated" or "Superseded" with a reference to its replacement. If the ADR
  is rejected, it must be marked with "Rejected".

* **Context:** This section describes the forces at play, including technical,
  political, social, and project local. These forces are probably in tension,
  and should be called out as such. The language in this section is
  value-neutral. It is simply describing facts.

* **Decision:** This section describes our response to these forces. It is
  stated in full sentences, with active voice. "We will …"

* **Consequences:** This section describes the resulting context, after applying
  the decision. All consequences should be listed here, not just the "positive"
  ones. A particular decision may have positive, negative, and neutral
  consequences, but all of them affect the team and project in the future.

The whole document should be one or two pages long. We will write each ADR as if
it is a conversation with a future developer. This requires good writing style,
with full sentences organized into paragraphs. Bullets are acceptable only for
visual style, not as an excuse for writing sentence fragments. (Bullets kill
people, even PowerPoint bullets.)
