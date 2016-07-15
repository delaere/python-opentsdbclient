        Meters is a vector dictionnaries.
        Meter dictionary *should* contain the following four required fields:

class metric:
    """Simple class to represent a metric"""
    
    def __init__(self,metric, timestamp, value, tags):
       """- metric: the name of the metric you are storing
          - timestamp: a Unix epoch style timestamp in seconds or milliseconds.
                       The timestamp must not contain non-numeric characters.
          - value: the value to record for this data point. It may be quoted or
                   not quoted and must conform to the OpenTSDB value rules.
          - tags: a map of tag name/tag value pairs. At least one pair must be
                  supplied."""
       
       self.metric = metric
       self.timestamp = timestamp
       self.value = value
       self.tags = tags

    def getMap(self):
        return { "metric": self.metric,
                 "value": self.value,
                 "tags": self.tags }

    def check(self):
        if (not isinstance(self.metric,basestring) or
            not isinstance(self.timestamp, int) or
            (not isinstance(self.value,basestring) and not isinstance(self.value, (int, float))) or
            not isinstance(self.tags, dict):
                raise TypeError("metric type mismatch.")
        if len(self.tags)<1:
                raise ValueError("at least one metric tag must be supplied")


