# Product Requirement Prompts

This file contains the requirements for your project. Fill out each section with relevant details before running `/init` in Claude Code. Claude will use this information to generate your initial project structure and implementation.

## Objective

```
# What is the main goal of this project? Be specific and concise.
# Example: "Build a REST API for managing customer orders with authentication"



```

## What

```
# Describe what the project should do in detail. List key features and functionality.
# Example: "- User registration and JWT authentication
#          - CRUD operations for orders
#          - Email notifications for order status changes"



```

## Why

```
# Explain the business value and problem this solves.
# Example: "Current manual order processing takes 30 minutes per order. 
#          This system will reduce processing time to under 2 minutes."



```

## Success criteria

```
# Define measurable success metrics and acceptance criteria.
# Example: "- All API endpoints respond in <200ms
#          - 100% test coverage for critical paths
#          - Handles 1000 concurrent users"



```

## Context (Optional)

```
# Add any additional context, constraints, or background information.
# Example: "Must integrate with existing PostgreSQL database
#          Deploy to AWS Lambda
#          Follow company REST API standards"



```

## Documentation and references (Optional)

```
# List any documentation, APIs, or resources that must be referenced.
# Include links, file paths, or specific sections to review.
# Example: "- AWS Lambda docs: https://docs.aws.amazon.com/lambda/
#          - Company API standards: ./docs/api-standards.md"



```

## Validation loop (Optional)

```
# Describe how to validate the implementation works correctly.
# Example: "1. Run test suite: npm test
#          2. Start local server: npm run dev
#          3. Test with Postman collection: ./tests/postman/"



```

## Syntax and style (Optional)

```
# Specify any coding standards or style preferences beyond the defaults.
# The template already includes Black, isort, and type hints.
# Example: "- Use async/await for all database operations
#          - Prefer composition over inheritance"



```

## Unit tests

```
# Describe the testing approach and any specific test cases needed.
# Example: "- Test all API endpoints with valid and invalid inputs
#          - Mock external services
#          - Test error handling and edge cases"



```

## Integration tests (Optional)

```
# Describe end-to-end testing requirements if applicable.
# Example: "- Test full order workflow from creation to delivery
#          - Test with real database (using test containers)
#          - Verify email notifications are sent"



```

## Security requirements (Optional)

```
# List specific security requirements beyond the template's built-in checks.
# Example: "- Implement rate limiting on all endpoints
#          - Audit log all data modifications
#          - Encrypt PII in database"



```