
def get_inner_spec(spec):
    brackets = 0
    offset = spec.find('(')
    if offset == -1:
        return ''
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if ch == '(':
            brackets += 1

        elif ch == ')':
            brackets -= 1

        if brackets == 0:
            break

    return spec[offset + 1:i]


def parse_arguments(param_string):
    """
    Given a string of function arguments, parse them into a tuple.
    """
    params = []
    bracket_level = 0
    current_param = ''

    for char in param_string:
        if char == '(':
            bracket_level += 1
        elif char == ')':
            bracket_level -= 1
        elif char == ',' and bracket_level == 0:
            params.append(current_param.strip())
            current_param = ''
            continue

        current_param += char

    # Append the last parameter
    if current_param:
        params.append(current_param.strip())

    return tuple(params)
