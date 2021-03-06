import json
from datetime import datetime
from datetime import timedelta
from numpy import mean
from collections import namedtuple
from os import linesep
from logger import get_logger


logger = get_logger('stream_log')
event_enum = namedtuple('Event', ['start', 'end', 'dur'])


def calculate_moving_average(events, dt_indexed_dur):
    """Calculates the moving average of events.
    Args:
        events (generator): the list of events with start date, end date and duration
        dt_indexed_dur (list): the list of duration with time sampled on breakdown

    Returns:
        list: list of dictionaries"""

    data = []
    for e in events:
        for id in dt_indexed_dur:
            # is start time between the timeslot
            cond1 = e.start >= id.start and e.start < id.end
            # is end time between the timeslot
            cond2 = e.end >= id.start and e.end < id.end
            # is there any overlap
            cond3 = e.end > id.end and e.start < id.end

            if any([cond1, cond2, cond3]):
                id.dur.append(e.dur)

    for id in dt_indexed_dur:
        data.append({
            id.end.strftime('%Y-%m-%d %H:%M:%S'): mean(id.dur) if id.dur else 0.0
        })
    
    return data


def process_event(event, window_min, window_max):
    """Filters only the events within the window..

    Args:
        event (string):  
        window_min (int): 
        window_max (int): 

    Returns:
        namedtuple: event with start_date, end_date and duration"""

    logger.info('Processing the Event: {}'.format(event))

    try:
        event = json.loads(event)
        start_date = datetime.strptime(event.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
        duration = event.get('duration') * event.get('nr_words', 1)
        end_date = start_date + timedelta(seconds=duration)

        # whether event start is between window
        cond1 = start_date >= window_min and start_date <= window_max
        # whether event end is between window
        cond2 = end_date >= window_min and end_date <= window_max
        # whether window is between event
        cond3 = end_date > window_max and start_date < window_min

        if any([cond1, cond2, cond3]):
            logger.info('Event is within the window')
            return event_enum(start_date, end_date, event.get('duration'))
        else:
            logger.info('Event is not within the window. So skipping it......')
    except Exception as ex:
        logger.error('Failed to process event: {}'.format(event), exc_info=True)


def read_events_in_window(file_path, window_min, window_max):
    with open(file_path, 'r') as fh:
        for line in fh:
            event = process_event(line, window_min, window_max)
            if event:
                yield event


def write_data(file_path, data):
    logger.info('Writing the aggregation to file {}'.format(file_path))

    with open(file_path, 'w') as fh:
        for d in data:
            try:
                fh.write(json.dumps(d))
                fh.write(linesep)
            except:
                logger.error('Exception occured for data: {}'.format(d))

    logger.info('Finished writing to file {}'.format(file_path))



def run(events_file, out_file, window_size, breakdown, end_date):
    # window_max = datetime.strptime('2018-12-26,18:24:00', '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    window_max = end_date
    window_min = window_max - timedelta(minutes=window_size)

    zip_ob = zip(range(0, window_size, breakdown), range(breakdown, window_size + 1, breakdown))

    dt_indexed_dur = [event_enum(window_max - timedelta(minutes=i[1]), window_max - timedelta(minutes=i[0]), []) for i in zip_ob]

    events = read_events_in_window(events_file, window_min, window_max)
    logger.info('Finished reading events from file.')

    write_data(out_file, calculate_moving_average(events, dt_indexed_dur))
