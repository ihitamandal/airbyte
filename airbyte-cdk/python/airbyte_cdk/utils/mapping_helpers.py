#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Optional, Set, Union


def combine_mappings(mappings: List[Optional[Union[Mapping[str, Any], str]]]) -> Union[Mapping[str, Any], str]:
    """
    Combine multiple mappings into a single mapping. If any of the mappings are a string, return
    that string. Raise errors in the following cases:
    * If there are duplicate keys across mappings
    * If there are multiple string mappings
    * If there are multiple mappings containing keys and one of them is a string
    """
    combined_mapping = {}
    seen_keys = set()
    string_option = None

    for mapping in mappings:
        if mapping is None:
            continue

        if isinstance(mapping, str):
            if string_option is not None:
                raise ValueError("Cannot combine multiple string options")
            string_option = mapping

        else:
            for key, value in mapping.items():
                if key in seen_keys:
                    raise ValueError(f"Duplicate keys found: {key}")
                seen_keys.add(key)
                combined_mapping[key] = value

    if string_option is not None:
        if combined_mapping:
            raise ValueError("Cannot combine multiple options if one is a string")
        return string_option

    return combined_mapping
