/**
 * ─────────────────────────────────────────────────────────────
 * Mullet Maters – Feed Service
 * Transparency Release (Production Code)
 * ─────────────────────────────────────────────────────────────
 *
 * This file contains the server-side implementation used by
 * Mullet Maters to construct the discovery / swipe feed.
 *
 * It is published to document the fairness and simplicity of
 * the feed logic, and to allow technically curious users,
 * journalists, and engineers to inspect how profiles are
 * selected and ordered.
 *
 * The feed intentionally does NOT:
 * - rank users by attractiveness
 * - rank users by payment or subscription status
 * - use engagement-based scoring
 * - use hidden boosts, penalties, or shadow rankings
 *
 * Ordering is based on:
 * - recent user activity
 * - geographic proximity
 * - user-selected filters
 *
 * This file intentionally excludes other production systems,
 * including (but not limited to):
 * - abuse detection and fraud prevention
 * - rate-limit enforcement internals
 * - moderation tooling
 * - App Attest verification logic
 * - operational monitoring and alerting
 *
 * Those systems exist to protect users and infrastructure and
 * are orthogonal to feed fairness.
 *
 * License:
 * This source file is licensed under the Business Source
 * License 1.1 (BSL 1.1).
 *
 * You may read, audit, and discuss this code for transparency
 * and evaluation purposes. You may not deploy this code as part
 * of a production dating service without an explicit license
 * from the copyright holder.
 *
 * © 2026 Mullet Maters
 */


import {BaseRpcService} from "../abc/base-rpc-service.js";
import {MMUser} from "../schema/mm-user.js";
import {RpcBuilder} from "../transport/rpc-builer.js";
import {BackendConfig} from "../backend.config.js";
import {MMUserProfile} from "../schema/mm-user-profile.js";
import PageResult from "../transport/page-result.js";
import PageBuilder from "../transport/page-builder.js";
import QueryCursor from "../transport/query-cursor.js";
import CustomDtoType from "../transport/custom-dto-type.js";
import {geohashRadiusBounds} from "../utils/geohash.js";
import {getFirestore} from "firebase-admin/firestore";
import {BlocklistService} from "./blocklist-service.js";
import {MMFeedFilters} from "../schema/mm-feed-filters.js";
import {MMUserSupplementals} from "../schema/mm-user-supplementals.js";
import {HttpsError} from "firebase-functions/https";
import {BackendDbSchema} from "../backend.schema.db.js";


export class FeedService extends BaseRpcService {
    static shared = new FeedService();

    static exports = Object.freeze({
        pullFeed: RpcBuilder
            .enter("pullFeed")
            .requiresAuth()
            .requiresAppCheck()
            .rateLimitPerUser({perDay: 500})
            .payloadFormat(CustomDtoType(od => ({
                filters: MMFeedFilters.from(od.refObj.filters),
                cursor: od.decodeField('cursor', 'string', true),
            })))
            .handle(ctx => FeedService.shared.pullFeed(ctx))
            .finalize()
    })

    constructor() {
        super();
    }

    // ─────────────────────────────────────────────────────────────
    // PUBLIC RPCs
    // Remotely invokable entry-points for this service.
    // All inputs are untrusted. Auth & validation required.
    // ─────────────────────────────────────────────────────────────

    /**
     * (PUBLIC)
     * Fetches the discovery/swipe feed for the caller
     * @param {RpcHandleContext<{filters: MMFeedFilters, cursor: string}>} ctx
     * @returns {Promise<{page: PageResult<MMUserProfile>, filters: MMFeedFilters}>}
     */
    async pullFeed({payload, callerId, db}) {
        const filters = payload.filters;
        let cursor = payload.cursor ? QueryCursor.fromToken(payload.cursor) : null;

        /** @type {Awaited<any[]>} */
        const supportingData = await Promise.all([
            db.collection(BackendDbSchema.$users).doc(callerId).get(),
            this.pullMaterializedSeenList(callerId),
            BlocklistService.shared.pullMaterializedBlocklist(callerId)
        ]);

        /** @type {import("firebase-admin/firestore").DocumentSnapshot} */
        const callerDoc = supportingData[0];
        const caller = MMUser.from(callerDoc.data());
        /** @type {Set<string>} */
        const seenSet = supportingData[1];
        /** @type {Set<string>} */
        const {blockedSet, blockedBySet} = supportingData[2];

        /** @type {MMUser[]} */
        const resultPool = [];
        const maxPageResultSize = 30;
        const minPageResultSize = 10;
        const maxScans = 50;
        let canExpandAgeBounds = !filters.useStrictAgeBounds && (filters.ageMin > 18 || filters.ageMax != null);
        let canExpandGeoRadius = !filters.useStrictGeoRadius && filters.radiusMi != null;
        let ageFuzz = 1;
        let geoFuzz = Math.max((filters.radiusMi ?? 0) * 0.5, 10);
        let nScans = 0;

        filters.genderId = caller.genderId;
        filters.genderPrefs = caller.genderPrefs;
        filters.geohash = caller.geohash;
        filters.intentPref = caller.intentPref;

        // Iteratively scan for profiles matching the caller's filters,
        // advancing the query cursor and expanding age bounds and geo radius
        // as needed or if applicable until we have a sizable result pool
        while (resultPool.length < minPageResultSize && nScans++ < maxScans) {
            /** @type {PageResult<MMUser>} */
            const pageResult = await this.#buildFeedQuery(cursor, filters)
                .limit(maxPageResultSize)
                .finalize(MMUser);

            resultPool.push(...pageResult.items.filter(user =>
                user.id !== callerId
                && !blockedSet.has(user.id)
                && !blockedBySet.has(user.id)
                && !seenSet.has(user.id)
                && user.isVisible
                && user.photos.length > 0
            ));
            resultPool.forEach((user) => {
                seenSet.add(user.id);
            });

            // Do not expand age or geo if we can advance the page instead
            if (pageResult.next != null) {
                cursor = QueryCursor.fromToken(pageResult.next);
                continue;
            }
            cursor = null;

            // Cannot advance current page. Break if not able to expand age or geo
            if (!canExpandAgeBounds && !canExpandGeoRadius) break;

            // Expand age bounds if able
            if (canExpandAgeBounds) filters.ageMin = Math.max(filters.ageMin - ageFuzz, 18);
            if (canExpandAgeBounds && filters.ageMax != null) filters.ageMax += ageFuzz;
            if (ageFuzz >= 4) canExpandAgeBounds = false;

            // Expand geo radius if able
            if (canExpandGeoRadius && filters.radiusMi != null) filters.radiusMi += geoFuzz;
            if (filters.radiusMi >= 100 || geoFuzz >= 100) filters.radiusMi = null; // nationwide/worldwide
            if (filters.radiusMi == null) canExpandGeoRadius = false;

            if (canExpandAgeBounds) ageFuzz *= 2;
            if (canExpandGeoRadius) geoFuzz *= 4;
        }

        const relativeUser = caller;
        const profiles = resultPool.map(user => user.toProfile({relativeUser}));
        const pageResult = new PageResult(profiles, null, cursor?.toToken() ?? null);
        return {page: pageResult.toWireJson(), filters: filters.toWireJson()}
    }


    // ─────────────────────────────────────────────────────────────
    // INTERNAL METHODS
    // Not remotely callable. Assumes trusted, validated inputs.
    // Do not expose directly without a public RPC wrapper.
    // ─────────────────────────────────────────────────────────────

    /**
     * (INTERNAL)
     * Retrieves the user's private materialized seen list document under their
     * user record and returns its contents as a Set for efficient lookup.
     * This is a derived, non-authoritative view of seen profiles.
     * @param {string} listOwnerUid
     * @returns {Promise<Set<string>>}
     * @throws {import("firebase-admin/firestore").FirebaseFirestoreError}
     */
    async pullMaterializedSeenList(listOwnerUid) {
        const mslDoc = getFirestore()
            .collection(BackendDbSchema.$users)
            .doc(listOwnerUid)
            .collection(BackendDbSchema.users.$docs)
            .doc(BackendDbSchema.users.docs.seenList$);

        const snap = await mslDoc.get();

        /** @type {string[]} */
        let seen = [];
        if (snap.exists) {
            const data = snap.data();
            seen = data.seen ?? [];
        }

        return new Set(seen);
    }

    /**
     * (INTERNAL)
     * Inserts UIDs into the user's private materialized seen list document
     * under their user record. Enforces a maximum of 320 entries via a
     * shift operation, evicting the oldest entry when the cap is reached.
     * This is a derived, non-authoritative structure used for feed filtering.
     * @param {string} listOwnerUid
     * @param {string} insertUids
     * @returns {Promise<void>}
     * @throws {import("firebase-admin/firestore").FirebaseFirestoreError}
     */
    async materializedSeenListInsert(listOwnerUid, ...insertUids) {
        if (insertUids.length === 0)
            return;

        const mslRef = getFirestore()
            .collection(BackendDbSchema.$users)
            .doc(listOwnerUid)
            .collection(BackendDbSchema.users.$docs)
            .doc(BackendDbSchema.users.docs.seenList$);

        await getFirestore().runTransaction(async (tx) => {
            const snap = await tx.get(mslRef);
            const data = snap.data() ?? {};

            /** @type {string[]} */
            const seen = data.seen ?? [];
            const seenSet = new Set(seen);

            const dedupedInsertUids = Array.from(new Set(insertUids))
                .filter(uid => !seenSet.has(uid));

            if (dedupedInsertUids.length === 0)
                return;

            const lim = 320;
            const overflow = seen.length + dedupedInsertUids.length - lim;
            const spliceCount = Math.max(0, overflow);

            seen.splice(0, spliceCount);
            seen.push(...dedupedInsertUids);
            tx.set(mslRef, { seen }, { merge: true });
        });
    }

    // ─────────────────────────────────────────────────────────────────────
    // PRIVATE METHODS
    // Not callable outside this service module.
    // Do not expose publicly or internally.
    // ─────────────────────────────────────────────────────────────────────

    /**
     * (PRIVATE)
     * Builds a configured PageBuilder for the feed query
     * based on the provided cursor and filters.
     * @param {QueryCursor | null} cursor
     * @param {MMFeedFilters} filters
     * @returns {PageBuilder}
     */
    #buildFeedQuery(cursor, filters) {
        /** @type {import("firebase-admin/firestore").Query} */
        let baseQuery = getFirestore().collection(BackendDbSchema.$users);

        const radiusMi = filters.radiusMi ?? Number.MAX_SAFE_INTEGER;
        const {geoMin, geoMax} = geohashRadiusBounds(filters.geohash, radiusMi);
        baseQuery = baseQuery
            .where(MMUser.Fields.geohash, '>=', geoMin)
            .where(MMUser.Fields.geohash, '<=', geoMax);

        const maxDob = new Date();
        maxDob.setFullYear(maxDob.getFullYear() - Math.max(filters.ageMin, 18));
        baseQuery = baseQuery.where(MMUser.Fields.dob, '<=', maxDob);

        const minDob = new Date();
        minDob.setFullYear(minDob.getFullYear() - (filters.ageMax ?? 1000));
        baseQuery = baseQuery.where(MMUser.Fields.dob, '>=', minDob);

        baseQuery = baseQuery
            .where(MMUser.Fields.genderId, 'in', filters.genderPrefs);
        baseQuery = baseQuery
            .where(MMUser.Fields.genderPrefs, 'array-contains', filters.genderId);

        baseQuery = baseQuery
            .where(MMUser.Fields.isVisible, '==', true);

        if (filters.intentPref == null)
            baseQuery = baseQuery
                .where(MMUser.Fields.intentPref, 'in', [
                    MMUserSupplementals.Basics.Intent.openTerm,
                    null
                ]);
        else if (filters.intentPref === MMUserSupplementals.Basics.Intent.openTerm)
            baseQuery = baseQuery
                .where(MMUser.Fields.intentPref, 'in', [
                    MMUserSupplementals.Basics.Intent.shortTerm,
                    MMUserSupplementals.Basics.Intent.longTerm,
                    MMUserSupplementals.Basics.Intent.openTerm,
                    null
                ]);
        else
            baseQuery = baseQuery
                .where(MMUser.Fields.intentPref, 'in', [
                    filters.intentPref,
                    MMUserSupplementals.Basics.Intent.openTerm,
                ]);

        if (filters.countryCode)
            baseQuery = baseQuery
                .where(MMUser.Fields.countryCode, '==', filters.countryCode);

        return PageBuilder
            .enter(baseQuery)
            .version(1)
            .cursor(cursor)
            .orderBy(MMUser.Fields.visitedAt, 'desc')
            .orderBy(MMUser.Fields.geohash, 'asc')
            .orderBy(MMUser.Fields.dob, 'asc')
            .orderBy('__name__', 'asc');
    }
}

/**
 * ─────────────────────────────────────────────────────────────
 * TRANSPARENCY TODO
 * ─────────────────────────────────────────────────────────────
 *
 * This file is intended to be published publicly as-is to
 * document how the Mullet Maters discovery feed works.
 *
 * Planned publication approach:
 *
 * 1) Create a small public repository, e.g.:
 *
 *      github.com/mulletmaters/mm-feed-transparency
 *
 *    Repository contents:
 *
 *      /
 *      ├─ feed-service.js        (this file, unmodified)
 *      ├─ SHA256.txt             (checksum of feed-service.js)
 *      └─ README.md              (brief explanation of scope)
 *
 * 2) Generate a SHA-256 checksum of this file:
 *
 *      shasum -a 256 feed-service.js > SHA256.txt
 *
 *    The checksum is included to allow verification that the
 *    published file is byte-for-byte identical across versions.
 *    This is primarily intended for technically literate users.
 *
 * 3) README.md should be intentionally short and factual.
 *    Suggested contents:
 *
 *      - What this repository is:
 *          "This repository contains the exact server-side
 *           logic used to generate the Mullet Maters discovery
 *           feed."
 *
 *      - What it is NOT:
 *          - It is not a full backend dump
 *          - It does not include abuse prevention
 *          - It does not include moderation logic
 *          - It does not include App Attest enforcement
 *
 *      - Why it exists:
 *          - To document feed fairness
 *          - To reduce speculation about hidden ranking
 *          - To provide auditability without exposing
 *            sensitive enforcement systems
 *
 *      - How to verify:
 *          - Recompute the SHA-256 checksum of feed-service.js
 *          - Compare against SHA256.txt
 *
 * 4) Do NOT make a marketing announcement.
 *    The repository should exist quietly for those who look.
 *
 * 5) If feed logic changes materially:
 *      - Update this file
 *      - Update SHA256.txt
 *      - Let git history speak for itself
 *
 * The goal is quiet, verifiable transparency — not performative
 * disclosure.
 */


