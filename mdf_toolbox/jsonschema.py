import json
import os

import jsonschema

from mdf_toolbox.json_dict import dict_merge, flatten_json


# *************************************************
# * JSONSchema utilities
# *************************************************

def condense_jsonschema(schema, include_containers=True, list_items=True):
    """Condense a JSONSchema into a dict of dot-notated data fields and data types.
    This strips out all of the JSONSchema directives, like ``required`` and
    ``additionalProperties``, leaving only the fields that could actually be found in valid data.

    Caution:
        This tool is not exhaustive, and will not work correctly on all JSONSchemas.
        In particular, schemas with objects nested in arrays will not be handled correctly,
        and data fields that do not have a listed ``type`` will be skipped.
        Additionally, ``$ref`` elements WILL NOT be expanded. Use ``expand_jsonschema()`` on your
        schema first if you want references expanded.

    Arguments:
        schema (dict): The JSONSchema to condense. ``$ref`` elements will not be expanded.
        include_containers (bool): Should containers (dicts/objects, lists/arrays) be listed
                separately from their fields?
                **Default**: ``True``, which will list containers.
        list_items (bool): Should the field ``items`` be included in the data fields?
                ``items`` denotes an array of things, and it not directly a data field,
                but the output can be confusing without it.
                **Default**: ``True``.

    Returns:
        dict: The list of data fields, in dot notation, and the associated data type
                (when specified), as `data_field: data_type`.
    """
    data_fields = {}
    for field, value in flatten_json(schema, True).items():
        # TODO: Make this logic more robust (and less hacky).
        #       Generally works for MDF purposes; will explode on complex JSONSchemas.
        if field.endswith("type") and (include_containers
                                       or (value != "object" and value != "array")):
            clean_field = field.replace("properties.", "").replace(".type", "")
            if not list_items:
                clean_field = clean_field.replace(".items", "")
            data_fields[clean_field] = str(value)
    return data_fields


def expand_jsonschema(schema, base_uri=None, definitions=None, resolver=None):
    """Expand references in a JSONSchema and return the dereferenced schema.
    Note:
        This function only dereferences simple ``$ref`` values. It does not
        dereference ``$ref`` values that are sufficiently complex.
        This tool is not exhaustive for all valid JSONSchemas.

    Arguments:
        schema (dict): The JSONSchema to dereference.
        base_uri (str): The base URI to the schema files, or a local path to the schema files.
                Required if ``resolver`` is not supplied (``base_uri`` is preferable).
        definitions (dict): Referenced definitions to start. Fully optional.
                **Default:** ``None``, to automatically populate definitions.
        resolver (jsonschema.RefResolver): The RefResolver to use in resolving ``$ref``
                values. Generally should not be set by users.
                **Default:** ``None``.

    Returns:
        dict: The dereferenced schema.
    """
    if not isinstance(schema, dict):
        return schema  # No-op on non-dict

    if resolver is None and base_uri is None:
        raise ValueError("base_uri is a required argument.")
    # Create RefResolver
    elif resolver is None:
        if os.path.exists(base_uri):
            base_uri = "{}{}{}".format(
                                    "file://" if not base_uri.startswith("file://") else "",
                                    os.path.abspath(base_uri),
                                    "/" if base_uri.endswith("/") else "")
        resolver = jsonschema.RefResolver(base_uri, None)

    if definitions is None:
        definitions = {}
    # Save schema's definitions
    # Could results in duplicate definitions, which has no effect
    if schema.get("definitions"):
        definitions = dict_merge(schema["definitions"], definitions)
        definitions = expand_jsonschema(definitions, definitions=definitions, resolver=resolver)
    while "$ref" in json.dumps(schema):
        new_schema = {}
        for key, val in schema.items():
            if key == "$ref":
                # $ref is supposed to take precedence, and effectively overwrite
                # other keys present, so we can make new_schema exactly the $ref value
                filename, intra_path = val.split("#")
                intra_parts = [x for x in intra_path.split("/") if x]
                # Filename ref refers to external file - resolve with RefResolver
                if filename:
                    ref_schema = resolver.resolve(filename)[1]
                    '''
                    with open(os.path.join(base_path, filename)) as schema_file:
                        ref_schema = json.load(schema_file)
                    '''
                    if ref_schema.get("definitions"):
                        definitions = dict_merge(ref_schema["definitions"], definitions)
                        definitions = expand_jsonschema(definitions, base_uri, definitions)
                    for path_part in intra_parts:
                        ref_schema = ref_schema[path_part]
                    # new_schema[intra_parts[-1]] = ref_schema
                    new_schema = ref_schema
                # Other refs should be in definitions block
                else:
                    if intra_parts[0] != "definitions" or len(intra_parts) != 2:
                        raise ValueError("Invalid/complex $ref: {}".format(intra_parts))
                    # new_schema[intra_parts[-1]] = definitions.get(intra_parts[1], "NONE")
                    new_schema = definitions.get(intra_parts[1], None)
                    if new_schema is None:
                        raise ValueError("Definition missing: {}".format(intra_parts))
            else:
                new_schema[key] = expand_jsonschema(val, definitions=definitions,
                                                    resolver=resolver)
        schema = new_schema
    return schema


def prettify_jsonschema(root, **kwargs):
    """Prettify a JSONSchema. Pretty-yield instead of pretty-print.

    Caution:
            This utility is not robust! It is intended to work only with
            a subset of common JSONSchema patterns (mostly for MDF schemas)
            and does not correctly prettify all valid JSONSchemas.
            Use with caution.

    Arguments:
        root (dict): The schema to prettify.

    Keyword Arguments:
        num_indent_spaces (int): The number of spaces to consider one indentation level.
                **Default:** ``4``
        bullet (bool or str): Will prepend the character given as a bullet to properties.
                When ``True``, will use a dash. When ``False``, will not use any bullets.
                **Default:** ``True``
        _nest_level (int): A variable to track the number of iterations this recursive
                functions has gone through. Affects indentation level. It is not
                necessary nor advised to set this argument.
                **Default:** ``0``

    Yields:
        str: Lines of the JSONschema. To print the JSONSchema, just print each line.
             Stylistic newlines are included as empty strings. These can be ignored
             if a more compact style is preferred.
    """
    indent = " " * kwargs.get("num_indent_spaces", 4)
    if kwargs.get("bullet", True) is True:
        bullet = "- "
    else:
        bullet = kwargs.get("bullet") or ""
    _nest_level = kwargs.pop("_nest_level", 0)
    # root should always be dict, but if not just yield it
    if not isinstance(root, dict):
        yield "{}{}".format(indent*_nest_level, root)
        return

    # If "properties" is a field in root, display that instead of root's fields
    # Don't change _nest_level; we're skipping this level
    if "properties" in root.keys():
        yield from prettify_jsonschema(root["properties"], _nest_level=_nest_level, **kwargs)
        if root.get("required"):
            yield "{}Required: {}".format(indent*_nest_level, root["required"])
        yield ""  # Newline
    # Otherwise display the actual properties
    else:
        for field, val in root.items():
            try:
                # Non-dict should just be yielded
                if not isinstance(val, dict):
                    yield "{}{}: {}".format(indent*_nest_level, field, val)
                    continue

                # Treat arrays differently - nesting is one level deeper
                if val.get("items"):
                    # Base information (field, type, desc)
                    yield ("{}{}{} ({} of {}): {}"
                           .format(indent*_nest_level, bullet, field, val.get("type", "any type"),
                                   val["items"].get("type", "any type"),
                                   val.get("description",
                                           val["items"].get("description", "No description"))))
                    # List item limits
                    if val.get("minItems") and val.get("maxItems"):
                        if val["minItems"] == val["maxItems"]:
                            yield ("{}{}Must have exactly {} item(s)"
                                   .format(indent*_nest_level, " "*len(bullet), val["minItems"]))
                        else:
                            yield ("{}{}Must have between {}-{} items"
                                   .format(indent*_nest_level, " "*len(bullet), val["minItems"],
                                           val["maxItems"]))
                    elif val.get("minItems"):
                        yield ("{}{}Must have at least {} item(s)"
                               .format(indent*_nest_level, " "*len(bullet), val["minItems"]))
                    elif val.get("maxItems"):
                        yield ("{}{}Must have at most {} item(s)"
                               .format(indent*_nest_level, " "*len(bullet), val["maxItems"]))
                    # Recurse through properties
                    if val["items"].get("properties"):
                        yield from prettify_jsonschema(val["items"]["properties"],
                                                       _nest_level=_nest_level+1, **kwargs)
                    # List required properties
                    if val["items"].get("required"):
                        yield ("{}Required: {}"
                               .format(indent*(_nest_level+1), val["items"]["required"]))
                    yield ""  # Newline
                else:
                    # Base information (field, type, desc)
                    yield ("{}{}{} ({}): {}"
                           .format(indent*_nest_level, bullet, field, val.get("type", "any type"),
                                   val.get("description", "No description")))
                    # Recurse through properties
                    if val.get("properties"):
                        yield from prettify_jsonschema(val["properties"],
                                                       _nest_level=_nest_level+1, **kwargs)
                    # List required properties
                    if val.get("required"):
                        yield "{}Required: {}".format(indent*(_nest_level+1), val["required"])
                    yield ""  # Newline

            except Exception as e:
                yield ("{}Error: Unable to prettify information for field '{}'! ({})"
                       .format(indent*_nest_level, field, e))
