from fire import Fire
from aggregate import run
from os.path import split, exists

def read_input(input_file, out_file, window_size, breakdown=1):
    """
    Args:
        input_file (string): Provide the full path of events file.
        out_file (string): Provide the full path of output file.
        window_size (number): Provide the window size in minutes.
        breakdown (number): Provide breakdown in minutes should be less than window_size.
    """
    if breakdown < 1 or not isinstance(breakdown, int):
        raise ValueError('Breakdown must be an integer and greater than 0.')

    if window_size < 1 or not isinstance(window_size, int):
        raise ValueError('Window size must be an integer and greater than 0.')

    if window_size < breakdown:
        raise ValueError('Window size cannot be lessthan breakdown')

    run(input_file, out_file, window_size, breakdown)

    return "Done..."


if __name__ == '__main__':
    x = Fire(read_input)
