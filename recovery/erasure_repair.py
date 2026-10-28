
import hashlib
import logging

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from erasure.coding import ErasureCoder
from metadata.database import SessionLocal
from metadata.models import ErasureStripe, ErasureFragment
from metadata.placement import NODE_URLS
from recovery.worker import (
    get_healthy_nodes,
    fetch_valid_chunk,
    write_replica
)


logger = logging.getLogger("erasure_repair")
coder = ErasureCoder()


def repair_erasure_stripe(db, stripe, healthy_nodes):
    records = {
        f.fragment_index: f
        for f in stripe.fragments
    }

    available = {}

    # Find and validate surviving fragments.
    for index, fragment in records.items():
        if fragment.node_id not in healthy_nodes:
            continue

        data = fetch_valid_chunk(
            fragment.node_id,
            fragment.fragment_hash
        )

        if data is not None:
            available[index] = data

    if len(available) == 3:
        return "healthy"

    if len(available) < 2:
        logger.error(
            "Stripe %s cannot be recovered: only %s fragments",
            stripe.id,
            len(available)
        )
        return "unrecoverable"

    missing_index = next(
        index for index in (0, 1, 2)
        if index not in available
    )

    missing_data = coder.repair(
        available, missing_index
    )

    missing_hash = hashlib.sha256(
        missing_data
    ).hexdigest()

    old_record = records.get(missing_index)

    if old_record is not None:
        if missing_hash != old_record.fragment_hash:
            logger.error(
                "Reconstruction checksum mismatch: stripe %s",
                stripe.id
            )
            return "unrecoverable"

    # Verify the entire reconstructed original stripe.
    try:
        candidate = dict(available)
        candidate[missing_index] = missing_data

        coder.decode(
            candidate,
            stripe.original_size,
            stripe.original_hash
        )

    except ValueError:
        logger.error(
            "Original stripe verification failed: %s",
            stripe.id
        )
        return "unrecoverable"

    # A stripe must use three distinct storage nodes.
    occupied_nodes = {
        records[index].node_id
        for index in available
    }

    candidates = [
        node_id for node_id in NODE_URLS
        if node_id in healthy_nodes
        and node_id not in occupied_nodes
    ]

    for target in candidates:
        if not write_replica(
            target,
            missing_hash,
            missing_data
        ):
            continue

        if old_record is None:
            db.add(
                ErasureFragment(
                    stripe_id=stripe.id,
                    fragment_index=missing_index,
                    fragment_hash=missing_hash,
                    node_id=target,
                    size=len(missing_data)
                )
            )
        else:
            old_record.node_id = target
            old_record.fragment_hash = missing_hash
            old_record.size = len(missing_data)

        db.commit()

        logger.info(
            "Repaired stripe %s fragment %s on %s",
            stripe.id,
            missing_index,
            target
        )
        return "repaired"

    logger.warning(
        "Stripe %s degraded: no spare node available",
        stripe.id
    )
    return "degraded"


def run_erasure_repair_cycle():
    healthy_nodes = get_healthy_nodes()

    results = {
        "healthy": 0,
        "repaired": 0,
        "degraded": 0,
        "unrecoverable": 0,
        "errors": 0
    }

    with SessionLocal() as db:
        stripes = db.scalars(
            select(ErasureStripe)
            .options(
                selectinload(ErasureStripe.fragments)
            )
        ).all()

        for stripe in stripes:
            try:
                status = repair_erasure_stripe(
                    db,
                    stripe,
                    healthy_nodes
                )
                results[status] += 1

            except Exception:
                db.rollback()
                results["errors"] += 1
                logger.exception(
                    "Repair failed on stripe %s",
                    stripe.id
                )

    logger.info("Erasure repair results: %s", results)
    return results


if __name__ == "__main__":
    import time

    logging.basicConfig(level=logging.INFO)

    while True:
        try:
            run_erasure_repair_cycle()
        except Exception:
            logger.exception("Recovery cycle failed")

        time.sleep(10)
