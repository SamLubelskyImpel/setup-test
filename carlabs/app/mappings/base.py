from dataclasses import asdict


class BaseMapping:

    def as_dict(self):
        return asdict(self)

    @property
    def fields(self):
        return [f for f in self.as_dict().items() if f[1] is not None]