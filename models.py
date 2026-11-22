from tortoise.models import Model
from tortoise import fields


class ServerBinding(Model):
    # Removing the shared settings on a server change also clears this ownership.
    id = fields.IntField(primary_key=True, default=1)
    server_url = fields.TextField()

    class Meta:
        table = "server_binding"


class Gateway(Model):
    # Cached for offline restarts; the credential file decides which Gateway this PC is.
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)

    class Meta:
        table = "gateway"

    def __repr__(self):
        return f"<Gateway(id={self.id}, name={self.name})>"


class CachedDeviceConfiguration(Model):
    # One common configuration per server cache, not a copy per station.
    id = fields.IntField(primary_key=True, default=1)
    auction_name = fields.CharField(max_length=100, default="")
    vehicle_threshold = fields.IntField(default=50)
    vehicle_ready_seconds = fields.FloatField(default=1.0)
    vehicle_empty_seconds = fields.FloatField(default=1.0)
    card_prompt_interval = fields.FloatField(default=5.0)
    completion_interval = fields.FloatField(default=10.0)
    print_copies = fields.IntField(default=1)
    serial_read_timeout = fields.FloatField(default=2.0)
    serial_write_timeout = fields.FloatField(default=2.0)
    updated_at = fields.DatetimeField(null=True)

    class Meta:
        table = "device_configuration"


class WeighingStation(Model):
    id = fields.IntField(primary_key=True)
    gateway = fields.ForeignKeyField("models.Gateway", related_name="weighing_stations")
    name = fields.CharField(max_length=100)
    description = fields.TextField()
    serial_port = fields.CharField(max_length=100)
    serial_description = fields.CharField(max_length=100)
    serial_location = fields.CharField(max_length=100)
    serial_number = fields.CharField(max_length=100)
    serial_manufacturer = fields.CharField(max_length=100)

    class Meta:
        table = "weighing_station"

    def __repr__(self):
        return f"<WeighingStation(port={self.serial_port}, name={self.name})>"


class Record(Model):
    uuid = fields.UUIDField(primary_key=True)
    rfid_card_uid = fields.CharField(max_length=20)
    # Keep the server ID independent of the settings cache replaced during sync.
    station_id = fields.IntField()
    weight = fields.IntField()
    measured_at = fields.DatetimeField()

    class Meta:
        table = "record"

    @classmethod
    def pending_uploads(cls):
        return cls.filter(upload_result__record_id__isnull=True)

    def __repr__(self):
        return f"<Record(rfid_card_uid={self.rfid_card_uid}, weight={self.weight}, measured_at={self.measured_at})>"


class RecordUploadResult(Model):
    record = fields.OneToOneField("models.Record", related_name="upload_result", primary_key=True)
    weight = fields.IntField(null=True)
    measured_at = fields.DatetimeField(null=True)
    cancelled_at = fields.DatetimeField(null=True)
    daily_number = fields.IntField(null=True)
    number_date = fields.DateField(null=True)
    # A rejected input or conflicting identity needs review and ends automatic retries.
    error = fields.TextField(null=True)
    received_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "record_upload_result"


class Species(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)

    class Meta:
        table = "species"


class Producer(Model):
    id = fields.IntField(primary_key=True)
    uuid = fields.UUIDField(unique=True)
    name = fields.CharField(max_length=100)
    phone = fields.CharField(max_length=20, null=True)
    created_at = fields.DatetimeField()

    class Meta:
        table = "producer"


class RFIDCard(Model):
    id = fields.IntField(primary_key=True)
    uuid = fields.UUIDField(unique=True)
    uid = fields.CharField(max_length=50, unique=True)
    producer = fields.ForeignKeyField("models.Producer", related_name="rfid_cards")
    species = fields.ForeignKeyField("models.Species", related_name="rfid_cards", null=True)
    is_active = fields.BooleanField(default=True)
    issued_at = fields.DatetimeField()

    class Meta:
        table = "rfid_card"
