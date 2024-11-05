from tb_rest_client.models.models_pe import Alarm as RestAlarm, DeviceId


class Alarm:
    def __init__(
            self,
            id,
            title,
            message,
            level,
            latitude,
            longitude,
            record_date,
            date_of_creation,
            car_id,
            driver_first_name = '',
            driver_last_name = '',
            place = ''
    ):
        self.id = id
        self.title = title
        self.message = message
        self.level = 'WARNING' if level <= 6 else 'CRITICAL'
        self.latitude = latitude
        self.longitude = longitude
        self.record_date = int(record_date)
        self.date_of_creation = int(date_of_creation)
        self.car_id = car_id
        self.driver_first_name = driver_first_name
        self.driver_last_name = driver_last_name
        self.place = place

    def to_model(self):
        return {
            'id': self.id,
            'title': self.title,
            'message': self.message,
            'level': self.level,
            'lat': self.latitude,
            'lon': self.longitude,
            'record_date': self.record_date,
            'date_of_creation': self.date_of_creation,
            'car_id': self.car_id,
            'driver_first_name': self.driver_first_name,
            'driver_last_name': self.driver_last_name,
            'place': self.place
        }

    def to_rest_object(self, device_id: DeviceId):
        return RestAlarm(
            type=self.title,
            name=self.title,
            severity=self.level,
            acknowledged=False,
            cleared=False,
            start_ts=int(round(self.date_of_creation*1000)),
            propagate_to_owner=True,
            propagate_to_owner_hierarchy=True,
            propagate=True,
            propagate_to_tenant=True,
            propagate_relation_types=['string'],
            details={'message': self.message},
            originator=device_id,
            status='ACTIVE_UNACK'
        )
