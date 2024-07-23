from dataclasses import dataclass
from enum import Enum


class ImageCompatStrategy(Enum):
    FULL_REF = 0
    ONLY_TAG = 1
    ONLY_NAME = 2


class SpaceCompatStrategy(Enum):
    REVERSE_ID = 0b0001
    IMAGE = 0b0010
    VERSION = 0b0100
    BRANCH = 0b1000

@dataclass
class CompatabilityStrategy:
    image: int = ImageCompatStrategy.ONLY_NAME
    space: int = SpaceCompatStrategy.REVERSE_ID
    none_is_always_compatible: bool = False



def compare_images(
    image_ref: str,
    other_image_ref: str,
    strategy: CompatabilityStrategy
) -> bool:
    if image_ref is None or other_image_ref is None:
        return strategy.none_is_always_compatible
    if strategy.image == ImageCompatStrategy.FULL_REF:
        return image_ref == other_image_ref
    elif strategy.image == ImageCompatStrategy.ONLY_TAG:
        return image_ref.split(":")[1] == other_image_ref.split(":")[1]
    elif strategy.image == ImageCompatStrategy.ONLY_NAME:
        return image_ref.split(":")[0] == other_image_ref.split(":")[0]
    else:
        raise ValueError("Invalid strategy")
