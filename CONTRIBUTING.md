# Contributing to the Project

## Getting Started

1. Create a [Github account](https://github.com/signup/free).
2. [Fork](https://help.github.com/articles/fork-a-repo/) the repository on Github.
3. Implement your feature / bugfix
4. Submit a [pull request](https://help.github.com/articles/creating-a-pull-request/) against the project for review.

## Style Guide

All commits to the project must pass linting and style guidelines. 

### Perl
- Install the modules listed in the `recommended_requires` section of [Build.PL](./Build.PL)
- Code should be formatted using the `.perltidyrc` in the project's root and the Perl::Tidy tool.
    - If the build fails due to a failed style check, try running `tidyall -a` and recommitting.
- Code should pass Perl::Critic with no errors/warnings using the `.perlcriticrc` in the project's root.
    - Any exceptions should be documented, noted in the pull request, and an exception added to the `.perlcriticrc` file.
- Please Build the project and make sure all meta files are up to date
	- See https://metacpan.org/pod/Module::Build for more info on Module::Build 
- To run style and linting checks, set the TEST_AUTHOR environment variable to a true value

```powershell
# Windows Powershell
$Env:TEST_AUTHOR = 1
```

```bash
# *nix
export TEST_AUTHOR=1
```

```powershell
perl ./Build.PL   # Build new Build file
./Build clean     # Clean up build files
./Build           # Build code and docs
./Build manifest  # Rebuild manifest
./Build test      # Run test suite
./Build distmeta  # Update metadata

./Build dist      # If you would also like to create a tar.gz

```

#### Suggested tools
#### vscode
If you are using [vscode](https://code.visualstudio.com/) as your development environment, the following extensions are recommended to ease linting tasks:
- https://marketplace.visualstudio.com/items?itemName=sfodje.perlcritic
- https://marketplace.visualstudio.com/items?itemName=sfodje.perltidy

