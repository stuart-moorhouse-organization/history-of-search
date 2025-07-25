# About This Claude Code Python Template

This template provides a production-ready starting point for Python projects using Claude Code. It includes modern Python tooling, security best practices, and automated workflows designed to help you build secure, maintainable Python applications.

## Template Contents

### 📁 Directory Structure

```
claude-code-template/
├── .claude/                    # Claude Code configuration
│   ├── settings.json          # Hooks and custom commands config
│   ├── commands/              # Custom slash commands
│   │   ├── security-review.md # Security analysis command
│   │   └── fix-github-issue.md # GitHub issue automation
│   ├── hooks/                 # Automation scripts
│   │   ├── pre-commit-security.sh # Pre-operation security checks
│   │   └── format-code.sh     # Auto-format on file write
│   └── logs/                  # Operation logs (auto-created)
├── .github/
│   └── workflows/
│       └── security.yml       # CI/CD pipeline
├── .git/
│   └── hooks/
│       └── pre-commit         # Git pre-commit checks
├── src/                       # Your source code
├── tests/                     # Your test files
├── docs/                      # Documentation
├── .gitignore.template        # Rename this to .gitignore for your project
├── pre-commit-config.yaml     # Pre-commit framework config
├── pyproject.toml             # Python project configuration
├── CLAUDE.md                  # Instructions for Claude
└── README.md                  # Project documentation
```

### 🔧 Key Features

#### 1. **Modern Python Packaging with UV**
- Uses `pyproject.toml` for dependency management
- UV for fast, reliable package management
- Separate dev dependencies for clean production installs
- Configured for Python 3.8+

#### 2. **Security-First Development**
- **Bandit**: Scans for common security issues in Python code
- **Safety**: Checks dependencies for known vulnerabilities
- **Ruff**: Fast linter with security rules enabled
- **Pre-commit hooks**: Prevents committing secrets or debug code
- **TruffleHog**: Scans for hardcoded secrets in CI/CD

#### 3. **Code Quality Tools**
- **Black**: Opinionated code formatter (88 char line length)
- **isort**: Automatic import sorting
- **Ruff**: Fast, comprehensive linting
- **MyPy**: Static type checking
- **pytest**: Testing with coverage reporting

#### 4. **Claude Code Integration**
- **Custom Commands**:
  - `/security-review`: Performs comprehensive security analysis
  - `/fix-github-issue <number>`: Automates GitHub issue fixes
- **Automated Hooks**:
  - Security checks before file operations
  - Auto-formatting after file writes
  - Logging of all operations

#### 5. **GitHub Actions Workflows**
The `.github/workflows/security.yml` file provides:
- **Security Scanning**: Runs on every push and PR
- **Code Quality Checks**: Formatting, linting, type checking
- **Test Coverage**: Automated testing with coverage reports
- **Dependency Scanning**: OWASP dependency check
- **Daily Security Audits**: Scheduled vulnerability scans

### 🚀 Getting Started

1. **Use this template** to create a new repository
2. **Clone** your new repository
3. **Fill out `product-requirement-prompts.md`** with your project details
   - Be specific about your objectives and success criteria
   - Include any relevant documentation or references
   - This file drives the `/init` command in Claude Code
4. **Run the setup**:
   ```bash
   # Install UV if needed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Set up the project
   uv venv
   uv pip install -e ".[dev]"
   pre-commit install
   ```
5. **Rename .gitignore.template to .gitignore**:
   ```bash
   mv .gitignore.template .gitignore
   ```
   This ensures Claude-specific files stay out of your project repository
6. **Start Claude Code**: `claude`
7. **Initialize your project**: Use `/init` to generate initial code based on your requirements

### 🔒 Security Features Explained

#### Pre-commit Security Hook
Located in `.claude/hooks/pre-commit-security.sh`, this script:
- Scans for hardcoded secrets (API keys, passwords)
- Warns about sensitive file modifications
- Logs all operations for audit trails

#### Git Pre-commit Hook
The `.git/hooks/pre-commit` script:
- Runs security checks before commits
- Detects debug statements (print, pdb, breakpoint)
- Executes tests automatically

#### GitHub Actions Security Workflow
The CI/CD pipeline includes:
- **Multiple security scanners**: Bandit, Ruff, Safety
- **Secret detection**: TruffleHog integration
- **Automated PR comments**: Security summaries
- **Artifact storage**: Detailed security reports

### 📝 Configuration Files

#### pyproject.toml
- Project metadata and dependencies
- Tool configurations (Black, isort, Ruff, pytest)
- Build system settings

#### pre-commit-config.yaml
- Automated checks before commits
- Secret detection
- Code formatting
- File integrity checks

#### .claude/settings.json
- Hook configurations for Claude Code
- Custom command registrations
- Tool automation settings

### 🛠️ Customization Guide

#### Adding Dependencies
```bash
# Add runtime dependency
uv pip install package-name
uv pip freeze | grep package-name >> pyproject.toml

# Add dev dependency
# Edit pyproject.toml and add to [project.optional-dependencies.dev]
```

#### Creating New Commands
1. Add a markdown file to `.claude/commands/`
2. Update `.claude/settings.json` to register it
3. Use with `/your-command` in Claude Code

#### Modifying Hooks
- Edit scripts in `.claude/hooks/`
- Ensure they remain executable
- Test thoroughly before committing

### 📚 Best Practices

1. **Always use type hints** - Helps catch bugs early
2. **Write tests first** - TDD improves code quality
3. **Document as you go** - Future you will thank you
4. **Review security reports** - Don't ignore warnings
5. **Keep dependencies updated** - Regular `uv pip install --upgrade`

### 🤝 Contributing to This Template

To improve this template:
1. Fork the repository
2. Make your enhancements
3. Test thoroughly
4. Submit a pull request

### 📄 License

This template is provided under the MIT License, making it free for both personal and commercial use.

### 🔗 Resources

- [UV Documentation](https://github.com/astral-sh/uv)
- [Claude Code Docs](https://docs.anthropic.com/claude-code)
- [Python Security Guide](https://python.readthedocs.io/en/latest/library/security_warnings.html)
- [OWASP Python Top 10](https://owasp.org/www-project-top-ten/)

---

**Note**: This template is designed to be a starting point. Adapt it to your specific needs while maintaining the security and quality practices it promotes.