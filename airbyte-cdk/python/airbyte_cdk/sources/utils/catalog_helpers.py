#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.models import AirbyteCatalog, SyncMode


class CatalogHelper:
    @staticmethod
    def coerce_catalog_as_full_refresh(catalog: AirbyteCatalog) -> AirbyteCatalog:
        """Update the sync mode on all streams in this catalog to be full refresh.

        Parameters
        ----------
        catalog : AirbyteCatalog
            The catalog with streams to be updated.

        Returns
        -------
        AirbyteCatalog
            The updated catalog with all streams set to full refresh sync mode.
        """
        # Direct in-place modification of streams
        for stream in catalog.streams:
            stream.source_defined_cursor = False
            stream.supported_sync_modes = [SyncMode.full_refresh]
            stream.default_cursor_field = None

        return catalog
