from tortoise.models import Model
from tortoise import fields


class Gateway(Model):
    id = fields.IntField(pk=True)
    mac_address = fields.CharField(max_length=17, unique=True)
    hostname = fields.CharField(max_length=255)
    ip_address = fields.CharField(max_length=39)
    name = fields.CharField(max_length=100)
    description = fields.TextField()
    access_token = fields.CharField(max_length=64)
    last_heartbeat = fields.DatetimeField(null=True)
    created_at = fields.DatetimeField()
    updated_at = fields.DatetimeField()

    class Meta:
        table = "gateway"

    def __repr__(self):
        return f"<Gateway(hostname={self.hostname}, name={self.name})>"


class WeighingStation(Model):
    id = fields.IntField(pk=True)
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
    uuid = fields.UUIDField(pk=True)
    rfid_card_uid = fields.CharField(max_length=20)
    weight = fields.IntField()
    measured_at = fields.DatetimeField()

    class Meta:
        table = "record"
    
    def __repr__(self):
        return f"<Record(rfid_card_uid={self.rfid_card_uid}, weight={self.weight}, measured_at={self.measured_at})>"


class Species(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)

    class Meta:
        table = "species"


class Producer(Model):
    id = fields.IntField(pk=True)
    uuid = fields.UUIDField(unique=True)
    name = fields.CharField(max_length=100)
    phone = fields.CharField(max_length=20, null=True)
    created_at = fields.DatetimeField()

    class Meta:
        table = "producer"


class RFIDCard(Model):
    id = fields.IntField(pk=True)
    uuid = fields.UUIDField(unique=True)
    uid = fields.CharField(max_length=50, unique=True)
    producer = fields.ForeignKeyField("models.Producer", related_name="rfid_cards")
    species = fields.ForeignKeyField("models.Species", related_name="rfid_cards")
    is_active = fields.BooleanField(default=True)
    issued_at = fields.DatetimeField()
    last_used_at = fields.DatetimeField(null=True)

    class Meta:
        table = "rfid_card"
