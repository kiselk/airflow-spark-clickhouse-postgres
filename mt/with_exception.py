
def critial_exit(step: str, e: Exception):
    print('Exception happened during [' + step + '], aborting...')
    print(str(e))
    raise(e)


def with_excepion(func):

    def inner_function(*args, **kwargs):

        f_name = str(func.__doc__)
        print("trying to execute: " + f_name)
        try:

            return func(*args, **kwargs)
        except Exception as e:
            critial_exit(
                f_name, e)

    return inner_function
