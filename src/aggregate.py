import json
from datetime import datetime
from datetime import timedelta
from numpy import mean
from collections import namedtuple
from os import linesep


event_tuple = namedtuple('Event', ['start', 'end', 'dur'])


def calculate_moving_average(events, dt_indexed_dur):

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
    event = json.loads(event)
    start_date = datetime.strptime(event.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
    duration = event.get('duration') * event.get('nr_words', 1)
    end_date = start_date + timedelta(seconds=duration)

    cond1 = start_date >= window_min and start_date <= window_max
    cond2 = end_date >= window_min and end_date <= window_max
    eval_cond = cond1 or cond2

    if eval_cond:
        return event_tuple(start_date, end_date, event.get('duration'))


def read_events_in_window(file_path, window_min, window_max):
    with open(file_path, 'r') as fh:
        for line in fh:
            event = process_event(line, window_min, window_max)
            if event:
                yield event


def write_data(file_path, data):
    with open(file_path, 'w') as fh:
        for d in data:
            fh.write(json.dumps(d))
            fh.write('\n')


def run(events_file, out_file, window_size, breakdown):
    window_max = datetime.strptime('2018-12-26 18:24:00', '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    window_min = window_max - timedelta(minutes=window_size)

    zip_ob = zip(range(0, window_size, breakdown), range(breakdown, window_size + 1, breakdown))

    dt_indexed_dur = [event_tuple(window_max - timedelta(minutes=i[1]), window_max - timedelta(minutes=i[0]), []) for i in zip_ob]

    events = read_events_in_window(events_file, window_min, window_max)
    
    write_data(out_file, calculate_moving_average(events, dt_indexed_dur))

