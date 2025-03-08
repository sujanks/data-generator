# Data Generator

A flexible and powerful data generation tool built in Go that generates synthetic data based on YAML manifest definitions.

## Features

- 🚀 High-performance batch data generation
- 📝 YAML-based manifest configuration
- 🔄 Support for table dependencies and relationships
- ✨ Rich data type support including JSON
- 🎯 Customizable data patterns and distributions
- ✅ Data validation and constraints

## Quick Start

1. Define your data schema in a YAML manifest file:

```yaml
tables:
- name: users
  priority: 1
  columns:
  - name: id
    pattern: "USER####"
    parent: true
    validation:
      unique: true
  - name: name
    type: string
    value: ["John Doe", "Jane Doe"]
  - name: metadata
    type: json
    json_config:
      min_keys: 2
      max_keys: 4
      fields: ["age", "email", "preferences"]
      types: ["int", "email", "string"]
  - name: created_at
    type: timestamp
    format: "2006-01-02 15:04:05"
```

2. Run the generator:

```bash
go run main.go -manifest manifest/application.yaml -count 1000
```

## Supported Data Types

- `string`: Basic string values
- `int`: Integer values with range support
- `decimal`: Decimal numbers with precision
- `timestamp`: Date and time with format and range
- `bool`: Boolean values
- `uuid`: Unique identifiers
- `sentence`: Random sentence generation
- `pattern`: Custom pattern-based strings (e.g., "ABC#####")
- `json`: Nested JSON objects with configurable fields

## Manifest Configuration

### Table Configuration

```yaml
tables:
  - name: table_name        # Table name
    priority: 1            # Processing priority (higher numbers = higher priority)
    depends_on: other_table # Table dependency
    validation:
      min_records: 1       # Minimum records to generate
      max_records: 1000    # Maximum records to generate
```

### Column Configuration

```yaml
columns:
  - name: column_name      # Column name
    type: string          # Data type
    pattern: "ABC####"    # Pattern for generated values
    value: ["A", "B"]     # Predefined values
    mandatory: true       # Required field
    validation:
      unique: true        # Unique constraint
    range:                # Value range
      min: 1
      max: 100
    format: "format_string" # Format specification
```

### JSON Configuration

```yaml
columns:
  - name: metadata
    type: json
    json_config:
      min_keys: 2          # Minimum number of keys in JSON
      max_keys: 5          # Maximum number of keys in JSON
      fields:              # Predefined field names
        - name
        - age
        - email
      types:               # Corresponding field types
        - string
        - int
        - email
```

Supported JSON field types:
- `string`: Random words
- `int`: Integer numbers (0-1000)
- `float`: Floating point numbers (0-1000)
- `bool`: Boolean values
- `date`: Date strings
- `email`: Email addresses
- `url`: URLs

## Features

### Data Validation
- Unique value constraints
- Min/max record counts
- Mandatory field validation
- Range validation for numeric and date fields

### Relationships
- Table dependencies
- Foreign key relationships
- Parent-child relationships

### Performance
- Batch processing
- Configurable batch sizes
- Efficient memory usage

### Data Distribution
- Weighted random values
- Custom value distributions
- Range-based generation

### JSON Generation
- Configurable number of fields
- Predefined or random field names
- Multiple value types
- Nested structure support

## Example Use Cases

1. **User Profile Generation**:
```yaml
- name: users
  columns:
    - name: id
      pattern: "U####"
      validation:
        unique: true
    - name: profile
      type: json
      json_config:
        fields: ["age", "location", "interests"]
        types: ["int", "string", "string"]
```

2. **Related Tables**:
```yaml
- name: orders
  depends_on: users
  columns:
    - name: order_id
      pattern: "ORD####"
    - name: user_id
      foreign: "users.id"
    - name: metadata
      type: json
      json_config:
        fields: ["items", "total", "shipping"]
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 