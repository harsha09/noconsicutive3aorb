from fire import Fire
from aggregate import run
from os.path import split, exists


def read_input(input_file, out_file, window_size, breakdown=1):
    """
    Args:
        input_file (string): provide the full path of events file
        out_file (string): provide the full path of output file
        window_size (number): provide the window size in minutes
        breakdown (number): provide breakdown in minutes should be less than window_size
    Raises:
        FileNotFoundError: if input file or output folder doesn't exist
        ValueError: if breakdown or window_size are not integers
                    if window_size < breakdown
                    if breakdown or window_size are < 1
                    if output filename is not given
    Returns:
        string: always Done...
    """

    if not exists(input_file):
        raise FileNotFoundError('Please provide valid input file path.')

    if not exists(split(out_file)[0]):
        raise FileNotFoundError('Please provide path to write output file.')

    if len(split(out_file)) < 1:
        raise ValueError('Please provide output filename.')

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
