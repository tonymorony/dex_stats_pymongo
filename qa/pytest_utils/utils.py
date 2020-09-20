import jsonschema


def validate_template(json: dict, schema: dict) -> bool:
    try:
        jsonschema.validate(instance=json, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        print("Validation failed :", e)
        return False
    return True
