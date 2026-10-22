from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from tortoise.models import Model
from tortoise.transactions import in_transaction

from cache import RFIDInfo
from models import Gateway, Producer, RFIDCard, Species, WeighingStation


def _identifier(value, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _rows(value, label: str) -> list[dict]:
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError(f"{label} must be a list of objects")
    return value


def _validate(model: type[Model], **values):
    # Tortoise coerces primitive values (for example, a dict to a port string).
    # REST responses must have their JSON field types before model conversion.
    for name, value in values.items():
        field_type = model._meta.fields_map[name].field_type
        expected_type = str if field_type in (datetime, UUID) else field_type
        if value is not None and expected_type in (str, int, bool) and type(value) is not expected_type:
            raise ValueError(f"{model.__name__}.{name} must be {expected_type.__name__}")
    instance = model(**values)
    # Run the model's storage validation before entering the replacement transaction.
    for name in instance._meta.fields_db_projection:
        field = instance._meta.fields_map[name]
        value = getattr(instance, name)
        if value is None and not field.null:
            raise ValueError(f"{type(instance).__name__}.{name} is required")
        field.to_db_value(value, instance)
    return instance


def _unique(instances, *fields: str):
    for field in fields:
        values = [getattr(instance, field) for instance in instances]
        if len(values) != len(set(values)):
            raise ValueError(f"Duplicate {field} in settings response")


@dataclass
class SettingsSnapshot:
    _gateway: Gateway
    stations: list[WeighingStation]
    _species: list[Species]
    _producers: list[Producer]
    _rfid_cards: list[RFIDCard]
    rfid_map: dict[str, RFIDInfo]

    @property
    def gateway_id(self) -> int:
        return self._gateway.id

    @property
    def gateway_name(self) -> str:
        return self._gateway.name

    @classmethod
    def from_responses(cls, gateway, stations, species, producers, rfid_cards):
        if not isinstance(gateway, dict):
            raise ValueError("Gateway must be an object")
        # The credential selects the Gateway; keep only what offline restarts need.
        gateway_model = _validate(Gateway,
            id=_identifier(gateway["id"], "Gateway ID"), name=gateway["name"],
        )

        station_models = []
        for row in _rows(stations, "Stations"):
            if _identifier(row["gateway"], "Station gateway ID") != gateway_model.id:
                raise ValueError("Station does not belong to this gateway")
            station_models.append(_validate(WeighingStation,
                id=_identifier(row["id"], "Station ID"), gateway_id=gateway_model.id,
                name=row["name"], description=row["description"],
                serial_port=row["serial_port"], serial_description=row["serial_description"],
                serial_location=row["serial_location"], serial_number=row["serial_number"],
                serial_manufacturer=row["serial_manufacturer"],
            ))
        _unique(station_models, "id")

        species_models = [
            _validate(Species, id=_identifier(row["id"], "Species ID"), name=row["name"])
            for row in _rows(species, "Species")
        ]
        _unique(species_models, "id")
        producer_models = [
            _validate(Producer,
                id=_identifier(row["id"], "Producer ID"), uuid=row["uuid"],
                name=row["name"], phone=row.get("phone"), created_at=row["created_at"],
            )
            for row in _rows(producers, "Producers")
        ]
        _unique(producer_models, "id", "uuid")
        species_by_id = {item.id: item for item in species_models}
        producers_by_id = {item.id: item for item in producer_models}

        card_models = []
        rfid_map = {}
        for row in _rows(rfid_cards, "RFID cards"):
            producer_id = _identifier(row["producer"], "Card producer ID")
            species_id = _identifier(row["species"], "Card species ID")
            if producer_id not in producers_by_id or species_id not in species_by_id:
                raise ValueError("RFID card references missing market data")
            card = _validate(RFIDCard,
                id=_identifier(row["id"], "Card ID"), uuid=row["uuid"], uid=row["uid"],
                producer_id=producer_id, species_id=species_id, is_active=row["is_active"],
                issued_at=row["issued_at"],
            )
            card_models.append(card)
            rfid_map[card.uid] = RFIDInfo(
                is_active=card.is_active,
                producer_name=producers_by_id[producer_id].name,
                species_name=species_by_id[species_id].name,
            )
        _unique(card_models, "id", "uuid", "uid")

        return cls(
            gateway_model, station_models, species_models, producer_models, card_models, rfid_map,
        )

    async def persist(self):
        async with in_transaction() as connection:
            await RFIDCard.all().using_db(connection).delete()
            await Producer.all().using_db(connection).delete()
            await Species.all().using_db(connection).delete()
            await WeighingStation.all().using_db(connection).delete()
            await Gateway.all().using_db(connection).delete()

            await Gateway.bulk_create([self._gateway], using_db=connection)
            await WeighingStation.bulk_create(self.stations, using_db=connection)
            await Species.bulk_create(self._species, using_db=connection)
            await Producer.bulk_create(self._producers, using_db=connection)
            await RFIDCard.bulk_create(self._rfid_cards, using_db=connection)
