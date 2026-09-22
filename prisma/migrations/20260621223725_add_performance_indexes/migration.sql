-- CreateIndex
CREATE INDEX "blacklist_blacklistedAt_idx" ON "blacklist"("blacklistedAt" DESC);

-- CreateIndex
CREATE INDEX "card_mappings_linked_at_idx" ON "card_mappings"("linked_at" DESC);

-- CreateIndex
CREATE INDEX "kyc_records_status_submittedAt_idx" ON "kyc_records"("status", "submittedAt" ASC);

-- CreateIndex
CREATE INDEX "terminals_status_idx" ON "terminals"("status");

-- CreateIndex
CREATE INDEX "transactions_student_uid_synced_at_idx" ON "transactions"("student_uid", "synced_at" DESC);

-- CreateIndex
CREATE INDEX "transactions_terminal_id_synced_at_idx" ON "transactions"("terminal_id", "synced_at" DESC);

-- CreateIndex
CREATE INDEX "transactions_type_idx" ON "transactions"("type");

-- CreateIndex
CREATE INDEX "users_role_isVerified_idx" ON "users"("role", "isVerified");

-- CreateIndex
CREATE INDEX "wallets_is_linked_idx" ON "wallets"("is_linked");
