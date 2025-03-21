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

### Rules and Expressions

The data generator now supports powerful rule-based data generation with expressions. Rules can be defined at both column and table levels.

```yaml
rules:
  # Time-based rules
  - when: "fields.submitted_date <= fields.created_on || addDuration(fields.created_on, '2h') > fields.submitted_date"
    then:
      submitted_date: "${addDuration(fields.created_on, '2h')}"

  # Conditional value setting
  - when: "fields.salary > 50000"
    then:
      priority: "HIGH"
    otherwise:
      priority: "${fields.salary > 25000 ? 'MEDIUM' : 'LOW'}"
```

#### Expression Functions

Time Functions:
- `addDuration(time, duration)`: Add duration to time (e.g., '1h', '30m', '2h')
- `parseTime(layout, value)`: Parse time string using layout
- `format(time, layout)`: Format time using layout
- `now()`: Get current time

Math Functions:
- `min(a, b)`: Return minimum of two numbers
- `max(a, b)`: Return maximum of two numbers

String Functions:
- `contains(str, substr)`: Check if string contains substring
- `hasPrefix(str, prefix)`: Check if string starts with prefix
- `hasSuffix(str, suffix)`: Check if string ends with suffix
- `lower(str)`: Convert string to lowercase
- `upper(str)`: Convert string to uppercase
- `trim(str)`: Remove leading/trailing whitespace

#### Example Rules

1. **Time Constraints**:
```yaml
- when: "fields.submitted_date <= fields.created_on"
  then:
    submitted_date: "${addDuration(fields.created_on, '2h')}"
```

2. **Status-based Updates**:
```yaml
- when: 'fields.status == "COMPLETED"'
  then:
    completed_on: "${addDuration(fields.modified_on, '2h')}"
```

3. **Complex Conditions**:
```yaml
- when: 'fields.status == "IN_PROGRESS" && fields.priority == "HIGH"'
  then:
    completed_on: "${addDuration(fields.modified_on, '1h')}"
    modified_by: "John Doe"
  otherwise:
    completed_on: "${addDuration(fields.modified_on, '2h')}"
    modified_by: "Jane Doe"
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

## Data Sinks

### CSV Sink

The CSV sink allows you to output generated data to CSV files. Each table will be written to a separate CSV file in the specified output directory.

Example usage in your manifest:

```yaml
sink:
  type: csv
  config:
    output_dir: "./output"
```

Features:
- Automatic header generation based on column names
- Support for all data types including JSON fields
- Proper escaping and formatting of values
- Multiple table support with separate files
- Automatic output directory creation

The CSV files will be named after the table names (e.g., `users.csv`, `orders.csv`). Each file will include a header row with column names followed by the data rows.

JSON fields are formatted in a readable string format: `{key1:value1,key2:value2}`. 