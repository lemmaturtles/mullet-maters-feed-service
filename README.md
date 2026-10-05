# Mullet Maters Feed Service

This repository contains the published implementation of the discovery-feed microservice used by [Mullet Maters](https://mulletmaters.app/), a dating app. The service is deployed on Google Cloud Run and fetches and constructs the discovery feed presented to Mullet Maters clients.

The implementation is published primarily for transparency and inspection of how Mullet Maters selects discovery profiles. This is a source release of the relevant service, not a standalone, drop-in deployable application: `feed-service.js` imports backend modules that are not included here.

**This repository does not currently provide reproducible builds.** The published source does not independently verify which code is deployed in production or provide cryptographic proof of the production artifact.

The project is licensed under the [MIT License](LICENSE).
