"""Business rules applied to each group in each window."""""

import apache_beam as beam
from datetime import datetime
from dateutil import parser as dateparser


class BusinessRulesDoFn(beam.DoFn):
    """This DoFn applies some business rules to a group of messages in a window.

        It leverages the grouping by session done thanks to the Window applied in the pipeline.
        Here, we will be only handling messages that belong to the same session, identified by ride_id.
        """

    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.correct_timestamps = beam.metrics.Metrics.counter(self.__class__, 'correct_timestamps')
        self.wrong_timestamps = beam.metrics.Metrics.counter(self.__class__, 'wrong_timestamps')

    def process(self, element,
                window=beam.DoFn.WindowParam,
                pane_info=beam.DoFn.PaneInfoParam):
        # v_iter is an iterator, so it is consumed when it is traversed
        k, v_iter = element

        min_timestamp = None
        max_timestamp = None
        n = 0
        init_status = "NA"
        end_status = "NA"

        # Find the min and max timestamp in all the events in this session.
        # Then find out the status corresponding to those timestamps
        # (sessions should start with pickup and end with dropoff)
        for v in v_iter:
            if not min_timestamp:
                min_timestamp = self._parse_timestamp(v['timestamp'])
            if not max_timestamp:
                max_timestamp = self._parse_timestamp(v['timestamp'])
            event_timestamp = self._parse_timestamp(v['timestamp'])
            if event_timestamp <= min_timestamp:
                min_timestamp = event_timestamp
                init_status = v['ride_status']
            if event_timestamp >= max_timestamp:
                max_timestamp = event_timestamp
                end_status = v['ride_status']
            n += 1

        # Duration of this session
        duration = (max_timestamp - min_timestamp).total_seconds()

        if pane_info.timing == 0:
            timing = "EARLY"
        elif pane_info.timing == 1:
            timing = "ON TIME"
        elif pane_info.timing == 2:
            timing = "LATE"
        else:
            timing = "UNKNOWN"

        # Output record, including some info about the window bounds and trigger
        # (useful to diagnose how windowing is working)
        r = {
            'ride_id': k,
            'duration': duration,
            'min_timestamp': min_timestamp.isoformat(),
            'max_timestamp': max_timestamp.isoformat(),
            'count': n,
            'init_status': init_status,
            'end_status': end_status,
            'trigger': timing,  # early, on time (watermark) or late
            'window_start': window.start.to_rfc3339(),  # iso format for timestamp of window start
            'window_end': window.end.to_rfc3339()  # iso format timestamp of window end
        }

        yield r

    def _parse_timestamp(self, s):
        # Default value in case of error
        d = datetime(1979, 2, 4, 0, 0, 0)
        try:
            d = dateparser.parse(s)
            self.correct_timestamps.inc()
        except ValueError:
            self.wrong_timestamps.inc()
        return d