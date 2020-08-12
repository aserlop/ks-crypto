import datetime
from dateutil.relativedelta import relativedelta


def transform(self, f):
    """
    Aplica una función al objeto self
    :param self: objeto sobre el que aplicar la función f
    :param f: función
    :return: resultado de aplicar f sobre self
    """
    return f(self)


def generate_date_batches(min_date: datetime.date, max_date: datetime.date, duration: relativedelta):
    batch_max_date = min_date
    while batch_max_date < max_date:
        batch_min_date = batch_max_date
        batch_max_date = min(batch_min_date + duration, max_date)
        yield (batch_min_date, batch_max_date)