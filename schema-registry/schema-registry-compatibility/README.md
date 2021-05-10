# Schema Registry Compatibility
## Avro
### BACKWARD
Changes allowed:
- Add fields with default values.
- Remove fields.
### FORWARD
Changes allowed:
- Add fields (only grow schema).
### FULL
Changes allowed:
- Add fields with default values.