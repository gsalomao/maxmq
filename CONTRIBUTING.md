# MaxMQ Contributing Guide

Thank you for the interest in contribute to the ``MaxMQ`` project. To make the
process as seamless as possible, we recommend you read this contribution guide. 

## Development Workflow

Start by forking the MaxMQ GitHub repository, make changes in a branch and then
send a pull request. We encourage pull requests to discuss code changes. Here 
are the steps in details:

### Set up your MaxMQ GitHub Repository

Fork [MaxMQ upstream](https://github.com/gsalomao/maxmq/fork) source repository
to your own personal repository. Copy the URL of your MaxMQ fork (you will need
it for the `git clone` command below).

```sh
$ git clone <URL of your MaxMQ fork>
$ make build
```

### Set up git remote as ``upstream``

```sh
$ cd maxmq
$ git remote add upstream https://github.com/gsalomao/maxmq
$ git fetch upstream
$ git merge upstream/master
...
```

### Create your feature branch

Before making code changes, make sure you create a separate branch for these
changes.

```sh
git checkout -b my-new-feature
```

### Test MaxMQ server

After your code changes, make sure:

- To add test cases for the new code
- To run `make check`
- To run `make fmt` and `make imports`
- To run `make test` and `make e2e`
- To run `make build`

### Commit changes

After verification, commit your changes. Your commit messages must follow the
[Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/) standard.

```sh
git commit -m 'feat(mqtt): add some feature'
```

### Push to the branch

Push your locally committed changes to the remote origin (your fork).

```sh
git push origin my-new-feature
```

### Create a Pull Request

Pull requests can be created via GitHub. Refer to 
[this document](https://help.github.com/articles/creating-a-pull-request/) for 
detailed steps on how to create a pull request. After a Pull Request gets peer 
reviewed and approved, it will be merged.

## FAQs

### How does ``MaxMQ`` manage dependencies?

``MaxMQ`` uses `go mod` to manage its dependencies.
- Run `go get foo/bar` in the source folder to add the dependency to `go.mod` 
file.

To remove a dependency:

- Edit your code and remove the import reference.
- Run `go mod tidy` in the source folder to remove dependency from `go.mod` 
file.

### What are the coding guidelines for ``MaxMQ``?

``MaxMQ`` follows the Golang style. Refer: 
[Effective Go](https://github.com/golang/go/wiki/CodeReviewComments) article 
from Golang project. If you observe offending code, please feel free to send a 
pull request.
