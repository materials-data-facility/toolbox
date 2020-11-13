from collections.abc import Container, Iterable, Mapping
from copy import deepcopy


# *************************************************
# * JSON and dict utilities
# *************************************************

def dict_merge(base, addition, append_lists=False):
    """Merge one dictionary with another, recursively.
    Fields present in addition will be added to base if not present or merged
    if both values are dictionaries or lists (with append_lists=True). If
    the values are different data types, the value in addition will be discarded.
    No data from base is deleted or overwritten.
    This function does not modify either dictionary.
    Dictionaries inside of other container types (list, etc.) are not merged,
    as the rules for merging would be ambiguous.
    If values from base and addition are of differing types, the value in
    addition is discarded.

    This utility could be expanded to merge Mapping and Container types in the future,
    but currently works only with dict and list.

    Arguments:
        base (dict): The dictionary being added to.
        addition (dict): The dictionary with additional data.
        append_lists (bool): When ``True``, fields present in base and addition
                that are lists will also be merged. Extra values from addition
                will be appended to the list in base.

    Returns:
        dict: The merged base.
    """
    if not isinstance(base, dict) or not isinstance(addition, dict):
        raise TypeError("dict_merge only works with dicts.")

    new_base = deepcopy(base)
    for key, value in addition.items():
        # Simplest case: Key not in base, so add value to base
        if key not in new_base.keys():
            new_base[key] = value
        # If the value is a dict, and base's value is also a dict, merge
        # If there is a type disagreement, merging cannot and should not happen
        if isinstance(value, dict) and isinstance(new_base[key], dict):
            new_base[key] = dict_merge(new_base[key], value)
        # If value is a list, lists should be merged, and base is compatible
        elif append_lists and isinstance(value, list) and isinstance(new_base[key], list):
            new_list = deepcopy(new_base[key])
            [new_list.append(item) for item in value if item not in new_list]
            new_base[key] = new_list
        # If none of these trigger, discard value from addition implicitly

    return new_base


def flatten_json(unflat_json, flatten_lists=True):
    """Flatten a JSON document into dot notation, where nested dicts are represented with a period.

    Arguments:
        unflat_json (dict): The JSON to flatten.
        flatten_lists (bool): Should the lists be flattened? **Default:** ``True``.
                Lists are flattened by merging contained dictionaries,
                and flattening those. Terminal values (non-container types)
                are added to a list and set at the terminal value for the path.
                When this is ``False``, lists are treated as terminal values and not flattened.

    Returns:
        dict: The JSON, flattened into dot notation in a dictionary.
                If a non-container value was supplied to flatten (e.g. a string)
                the value will be returned unchanged instead.

    Warning:
        Mixing container and non-container types in a list is not recommended.
        (e.g. [{"key": "val"}, "other_val"])
        If a list mixes types in this way, the non-container values MAY be listed
        under the field "flatten_undefined".

    Examples::

        {
            "key1": {
                "key2": "value"
            }
        }
        turns into
        {
            "key1.key2": value
        }


        {
            "key1": {
                "key2": [{
                    "key3": "foo",
                    "key4": "bar"
                }, {
                    "key3": "baz"
                }]
            }
        }
        with flatten_lists=True, turns into
        {
            "key1.key2.key3": ["foo", "baz"],
            "key1.key2.key4": "bar"
        }
    """
    flat_json = {}
    # Dict flattens by keys
    if isinstance(unflat_json, dict):
        for key, val in unflat_json.items():
            flat_val = flatten_json(val, flatten_lists=flatten_lists)
            # flat_val is dict to add to flat_json
            if isinstance(flat_val, dict):
                for subkey, subval in flat_val.items():
                    if subkey != "flatten_undefined":
                        flat_json[key+"."+subkey] = subval
                    # "flatten_unknown" is from mixed-type lists (container and non-container)
                    # Attempt to fix. This is not guaranteed; recommend not mixing types
                    else:
                        flat_json[key] = subval
            # flat_val is a terminal value (anything besides dict)
            else:
                flat_json[key] = flat_val

    # List flattens by values inside
    elif flatten_lists and isinstance(unflat_json, list):
        # Dict of flat keys processed so far
        partial_flats = {}
        # List of terminal values
        terminals = []
        for val in unflat_json:
            flat_val = flatten_json(val, flatten_lists=flatten_lists)
            # flat_val is dict, need to appropriately merge
            if isinstance(flat_val, dict):
                for subkey, subval in flat_val.items():
                    # If subkey is duplicate, add values to list
                    if subkey in partial_flats.keys():
                        # Create list if not already
                        if type(partial_flats[subkey]) is not list:
                            partial_flats[subkey] = [partial_flats[subkey], subval]
                        else:
                            partial_flats[subkey].append(subval)
                    # If subkey not duplicate, just add
                    else:
                        partial_flats[subkey] = subval
            # flat_val is a terminal value (anything besides dict)
            # Lists should be merged into terminals
            elif isinstance(flat_val, list):
                terminals.extend(flat_val)
            # Non-containers just appended to terminals
            else:
                terminals.append(flat_val)

        # Clean up for returning
        # If only one of partial_flats and terminals is populated, return that,
        # but if neither are flattened return an empty dict (partial_flats)
        # partial_flats is all contained dicts, flattened
        if not terminals:
            flat_json = partial_flats
        # terminals is all contained terminal values (flat by definition)
        elif terminals and not partial_flats:
            # If only one value in terminals, just return it
            if len(terminals) == 1:
                terminals = terminals[0]
            flat_json = terminals
        # Otherwise, add in sentinel field "flatten_undefined"
        # This case only occurs when a non-container type is mixed with a container type
        # in a list (e.g. [{"key": "val"}, "other_val"]) and is removed at an earlier
        # recursion depth if possible
        else:
            if len(terminals) == 1:
                terminals = terminals[0]
            partial_flats["flatten_undefined"] = terminals
            flat_json = partial_flats

    # Not container; cannot flatten
    else:
        flat_json = unflat_json
    return flat_json


def insensitive_comparison(item1, item2, type_insensitive=False, string_insensitive=False):
    """Compare two items without regard to order.

    The following rules are used to determine equivalence:
        * Items that are not of the same type can be equivalent only when ``type_insensitive=True``.
        * Mapping objects are equal iff the keys in each item exist in both items and have
          the same value (with the same ``insensitive_comparison``).
        * Other containers except for strings are equal iff every element in each item exists
          in both items (duplicate items must be present the same number of times).
        * Containers must be ``Iterable`` to be compared in this way.
        * Non-containers are equivalent if the equality operator returns ``True``.
        * Strings are treated as non-containers when ``string_insensitive=False``,
          and are treated as containers when ``string_insensitive=True``. When treated as
          containers, each (case-insensitive) character is treated as an element and
          whitespace is ignored.
        * If the items are in different categories above, they are never equivalent,
          even when ``type_insensitive=True``.

    Arguments:
        item1 (any): The first item to compare.
        item2 (any): The second item to compare.
        type_insensitive (bool): When ``True``, items of a different type are not automatically
                unequivalent. When ``False``, items must be the same type to be equivalent.
                **Default**: ``False``.
        string_insensitive (bool): When ``True``, strings are treated as containers, with each
                character being one element in the container.
                When ``False``, strings are treated as non-containers and compared directly.
                **Default**: ``False``.

    Returns:
        bool: ``True`` iff the two items are equivalent (see above).
                ``False`` otherwise.
    """
    # If type-sensitive, check types
    if not type_insensitive and type(item1) != type(item2):
        return False

    # Handle Mapping objects (dict)
    if isinstance(item1, Mapping):
        # Second item must be Mapping
        if not isinstance(item2, Mapping):
            return False
        # Items must have the same number of elements
        if not len(item1) == len(item2):
            return False
        # Keys must be the same
        if not insensitive_comparison(list(item1.keys()), list(item2.keys()),
                                      type_insensitive=True):
            return False
        # Each key's value must be the same
        # We can just check item1.items because the keys are the same
        for key, val in item1.items():
            if not insensitive_comparison(item1[key], item2[key],
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive):
                return False
        # Keys and values are the same
        return True
    # Handle strings
    elif isinstance(item1, str):
        # Second item must be string
        if not isinstance(item2, str):
            return False
        # Items must have the same number of elements (except string_insensitive)
        if not len(item1) == len(item2) and not string_insensitive:
            return False
        # If we're insensitive to case, spaces, and order, compare characters
        if string_insensitive:
            # If the string is one character long, skip additional comparison
            if len(item1) <= 1:
                return item1.lower() == item2.lower()
            # Make strings into containers (lists) and discard whitespace
            item1_list = [c for c in item1.lower() if not c.isspace()]
            item2_list = [c for c in item2.lower() if not c.isspace()]
            # The insensitive args shouldn't matter, but they're here just in case
            return insensitive_comparison(item1_list, item2_list,
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive)
        # Otherwise, case and order matter
        else:
            return item1 == item2
    # Handle other Iterable Containers
    elif isinstance(item1, Container) and isinstance(item1, Iterable):
        # Second item must be an Iterable Container
        if not isinstance(item2, Container) or not isinstance(item2, Iterable):
            return False
        # Items must have the same number of elements
        if not len(item1) == len(item2):
            return False
        # Every element in item1 must be in item2, and vice-versa
        # Painfully slow, but unavoidable for deep comparison
        # Each match in item1 removes the corresponding element from item2_copy
        # If they're the same, item2_copy should be empty at the end,
        #   unless a .remove() failed, in which case we have to re-match using item2
        item2_copy = list(deepcopy(item2))
        remove_failed = False
        for elem in item1:
            matched = False
            # Try every element
            for candidate in item2:
                # If comparison succeeds, flag a match, remove match from copy, and dump out
                if insensitive_comparison(elem, candidate,
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive):
                    matched = True
                    try:
                        item2_copy.remove(candidate)
                    except ValueError:  # list.remove(x): x not in list
                        remove_failed = True
                    break
            # One failure indicates unequivalence
            if not matched:
                return False
        # If all removes succeeded, we can shortcut checking all item2 elements in item1
        if not remove_failed:
            # If the Containers are equivalent, all elements in item2_copy should be removed
            # Otherwise
            return len(item2_copy) == 0
        # If something failed, we have to verify all of item2
        # We can't assume item2 != item1, because removal is comparative
        else:
            for elem in item2:
                matched = False
                # Try every element
                for candidate in item1:
                    # If comparison succeeds, flag a match, remove match from copy, and dump out
                    if insensitive_comparison(elem, candidate,
                                              type_insensitive=type_insensitive,
                                              string_insensitive=string_insensitive):
                        matched = True
                        break
                # One failure indicates unequivalence
                if not matched:
                    return False
            # All elements have a match
            return True
    # Handle otherwise unhandled type (catchall)
    else:
        return item1 == item2


def prettify_json(root, **kwargs):
    """Prettify a JSON object or list. Pretty-yield instead of pretty-print.

    Arguments:
        root (dict): The JSON to prettify.

    Keyword Arguments:
        num_indent_spaces (int): The number of spaces to consider one indentation level.
                **Default:** ``4``
        inline_singles (bool): When ``True``, will give non-container values inline
                for dictionary keys (e.g. "key: value"). When ``False``, will
                give non-container values on a separate line, like container values.
                **Default:** ``True``
        bullet (bool or str): Will prepend the character given as a bullet to properties.
                When ``True``, will use a dash. When ``False``, will not use any bullets.
                **Default:** ``True``
        _nest_level (int): A variable to track the number of iterations this recursive
                functions has gone through. Affects indentation level. It is not
                necessary nor advised to set this argument.
                **Default:** ``0``

    Yields:
        str: Lines of the prettified JSON, which can be directly printed if desired.
             Stylistic newlines are included as empty strings. These can be ignored
             if a more compact style is preferred.
    """
    indent = " " * kwargs.get("num_indent_spaces", 4)
    inline = kwargs.get("inline_singles", True)
    if kwargs.get("bullet", True) is True:
        bullet = "- "
    else:
        bullet = kwargs.get("bullet") or ""
    _nest_level = kwargs.pop("_nest_level", 0)
    if not root and root is not False:
        root = "None"

    # Prettify key/value pair
    if isinstance(root, dict):
        for k, v in root.items():
            # Containers and non-inline values should be recursively prettified
            if not inline or isinstance(v, dict) or isinstance(v, list):
                # Indent/bullet + key name
                yield "{}{}{}:".format(indent*_nest_level, bullet, k)
                # Value prettified with additional indent
                yield from prettify_json(v, _nest_level=_nest_level+1, **kwargs)
            # Otherwise, can prettify inline
            else:
                pretty_value = next(prettify_json(v, bullet=False, _nest_level=0))
                yield "{}{}{}: {}".format(indent*_nest_level, bullet, k, pretty_value)
            yield ""  # Newline
    # Prettify each item
    elif isinstance(root, list):
        for item in root:
            # Prettify values
            # No additional indent - nothing at top-level
            yield from prettify_json(item, _nest_level=_nest_level, **kwargs)
    # Just yield item
    else:
        yield "{}{}{}".format(indent*_nest_level, bullet, root)


def translate_json(source_doc, mapping, na_values=None, require_all=False):
    """Translate a JSON document (as a dictionary) from one schema to another.

    Note:
        Only JSON documents (and therefore datatypes permitted in JSON documents)
        are supported by this tool.

    Arguments:
        source_doc (dict): The source JSON document to translate.
        mapping (dict): The mapping of destination_fields: source_fields, in
                dot notation (where nested dicts/JSON objects are represented with a period).
                Missing fields are ignored.

                Examples::

                    {
                        "new_schema.some_field": "old_schema.stuff.old_fieldname"
                    }
                    {
                        "new_doc.organized.new_fieldname": "old.disordered.vaguename"
                    }
        na_values (list): Values to treat as N/A (not applicable/available).
                N/A values will be ignored and not copied.
                **Default:** ``None`` (no N/A values).
        require_all (bool): Must every value in the mapping be found? **Default:** ``False``.
                It is advised to leave this false unless the translated document depends
                on every key's value being present. Even so, it is advised to use
                JSONSchema validation instead.

    Returns:
        dict: The translated JSON document.
    """
    if na_values is None:
        na_values = []
    elif not isinstance(na_values, list):
        na_values = [na_values]

    # Flatten source_doc - will match keys easier
    flat_source = flatten_json(source_doc)
    # For each (dest, source) pair, attempt to fetch source's value to add to dest
    dest_doc = {}
    for dest_path, source_path in flatten_json(mapping).items():
        try:
            value = flat_source[source_path]
            # Check that the value is valid to translate, including contained values
            if isinstance(value, list):
                while any([na in value for na in na_values]):
                    [value.remove(na) for na in na_values if na in value]
            if value not in na_values and value != []:
                # Determine path to add
                fields = dest_path.split(".")
                last_field = fields.pop()
                current_field = dest_doc
                # Create all missing fields
                for field in fields:
                    if current_field.get(field) is None:
                        current_field[field] = {}
                    current_field = current_field[field]
                # Add value to end
                current_field[last_field] = value

        # KeyError indicates missing value - only panic if no missing values are allowed
        except KeyError as e:
            if require_all:
                raise KeyError("Required key '{}' not found during translation of JSON "
                               "document:\n{}".format(source_path, source_doc)) from e
    return dest_doc
