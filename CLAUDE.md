# Claude Code Python Project Documentation

This document provides context and guidelines for Claude when working on this Python project.

## Project Overview

This is a Python project template for Claude Code that includes security best practices, automated formatting, and pre-commit hooks.

## Project Structure

- `src/` - Main source code directory
- `tests/` - Test files
- `docs/` - Documentation
- `.claude/` - Claude Code configuration and hooks
- `.github/` - GitHub-specific configurations
- `pyproject.toml` - Python project configuration and dependencies

## Development Guidelines

### Code Style
- Follow PEP 8 conventions
- Use Black for code formatting (88 character line length)
- Use isort for import sorting
- Use type hints for all functions
- Docstrings for all public functions and classes

### Security Practices
- Never hardcode secrets or credentials
- Use environment variables for sensitive data
- Validate all user inputs
- Follow OWASP security guidelines
- Use Bandit and Safety for security scanning

### Testing
- Write tests for all new features
- Maintain test coverage above 80%
- Use pytest for testing
- Run tests before committing: `uv run pytest`

### Package Management
- This project uses UV for Python package management
- Dependencies are defined in `pyproject.toml`
- Install dependencies with: `uv pip install -e ".[dev]"`

## Available Commands

### UV Commands
- `uv venv` - Create virtual environment
- `uv pip install -e ".[dev]"` - Install all dependencies
- `uv run pytest` - Run tests
- `uv run black src/` - Format code
- `uv run ruff check src/` - Lint code
- `uv run mypy src/` - Type check code

### Testing & Quality
- `uv run pytest` - Run all tests
- `uv run pytest --cov=src` - Run tests with coverage
- `uv run black --check src/` - Check formatting
- `uv run isort --check-only src/` - Check import sorting
- `uv run bandit -r src/` - Security scanning

## Custom Claude Commands

- `/security-review` - Perform comprehensive security analysis
- `/fix-github-issue <number>` - Automatically fix a GitHub issue

## Environment Variables

Create a `.env` file with:
```
# Add your environment variables here
API_KEY=your_api_key_here
DATABASE_URL=your_database_url_here
```

## Common Tasks

### Adding a New Feature
1. Create a feature branch
2. Implement the feature with tests
3. Run formatting: `uv run black src/`
4. Run linting: `uv run ruff check src/`
5. Run tests: `uv run pytest`
6. Create a pull request

### Setting Up Development Environment
1. Install UV: `curl -LsSf https://astral.sh/uv/install.sh | sh`
2. Create virtual environment: `uv venv`
3. Install dependencies: `uv pip install -e ".[dev]"`
4. Install pre-commit hooks: `pre-commit install`

### Debugging
1. Check logs in `.claude/logs/`
2. Use debugger with `breakpoint()` (Python 3.7+)
3. Remove all debug code before committing

## Project Initialization

When initializing this project with /init, refer to product-requirement-prompts.md for the specific requirements and success criteria. This file contains all the necessary context for generating the initial project structure and implementation.

## Important Notes

- Always check for existing implementations before creating new files
- Follow the principle of least privilege for file permissions
- Keep dependencies up to date with `uv pip install --upgrade`
- Document any complex logic or business rules
- Use type hints to improve code clarity and catch errors early