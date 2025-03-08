# Data Generator

A flexible and powerful data generation tool built in Go that generates synthetic data based on YAML manifest definitions.

## Features

- 🚀 High-performance batch data generation
- 📝 YAML-based manifest configuration
- 🔄 Support for table dependencies and relationships
- ✨ Rich data type support
- 🎯 Customizable data patterns and distributions
- ✅ Data validation and constraints


## Quick Start

1. Define your data schema in a YAML manifest file:

```yaml
tables:
- name: application
  priority: 1
  columns:
  - name: application_id
    pattern: "ABC#####"
    parent: true
    validation:
      unique: true  
  - name: modified_on
    type: timestamp
    format: "2025-03-07 12:00:00"
    range:
      min: "2025-03-07 12:00:00"
      max: "2025-03-07 12:00:00"
  - name: modified_by
    type: string
    value: ["John Doe", "Jane Doe"]
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 