This library implements a in-memory relationship-based access control dababase, that was inspired by [Google's Zanzibar](https://research.google/pubs/pub48190/).

# Naming
## `Object`
A `Object` is a tuple of the values (`namespace`, `id`).
It represents a object like a user.
Example: (`users`, `alice`).

## `Set`
A `Set` is a tuple of the values (`namespace`, `id`, `permission`).
It represents a permission for a `Object`.
Example: (`files`, `foo.pdf`, `read`).

# Usage
The `RelationGraph`-struct contains a graph of all relationships.
Relationships can be created between:
- `Object` and `Set` => user alice can read the file foo.pdf.
- `Set` and `Set` => everyone who can read the file foo.pdf can read the file bar.pdf.

# Specials
- The `*`-id is used as a wildcard id to create a virtual relation from this id to every other id in the namespace.
  Example: (`user`, `alice`) -> (`file`, `*`, `read`) => user alice can read every file



# Roadmap
- [ ] implement raft protocol to allow ha deployment

# Server
A basic gRPC based server for interacting with the database can be found in the git repository.

# Contributing
I'm happy about any contribution in any form.
Feel free to submit feature requests and bug reports using a GitHub Issue.
PR's are also appreciated.

# License
This Library is licensed under [LGPLv3](https://www.gnu.org/licenses/lgpl-3.0.en.html).
