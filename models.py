from tortoise.models import Model
from tortoise import fields


class Gateway(Model):
    # Cached for offline restarts; the credential file decides which Gateway this PC is.
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)

    class Meta:
        table = "gateway"

    def __repr__(self):
        return f"<Gateway(id={self.id}, name={self.name})>"


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
    weight = fields.IntField()
    measured_at = fields.DatetimeField()

    class Meta:
        table = "record"
    
    def __repr__(self):
        return f"<Record(rfid_card_uid={self.rfid_card_uid}, weight={self.weight}, measured_at={self.measured_at})>"


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
    species = fields.ForeignKeyField("models.Species", related_name="rfid_cards")
    is_active = fields.BooleanField(default=True)
    issued_at = fields.DatetimeField()

    class Meta:
        table = "rfid_card"
