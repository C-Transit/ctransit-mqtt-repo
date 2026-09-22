-- CreateEnum
CREATE TYPE "Role" AS ENUM ('STUDENT', 'ADMIN', 'AGENT', 'DRIVER');

-- CreateEnum
CREATE TYPE "KycStatus" AS ENUM ('PENDING', 'APPROVED', 'REJECTED');

-- CreateEnum
CREATE TYPE "TransactionType" AS ENUM ('RIDE', 'TOPUP', 'REFUND');

-- CreateEnum
CREATE TYPE "TerminalStatus" AS ENUM ('ONLINE', 'OFFLINE', 'LOCKED');

-- CreateEnum
CREATE TYPE "AgentStatus" AS ENUM ('ACTIVE', 'SUSPENDED', 'DEACTIVATED');

-- CreateEnum
CREATE TYPE "DisputeStatus" AS ENUM ('OPEN', 'UNDER_REVIEW', 'RESOLVED', 'REJECTED');

-- CreateTable
CREATE TABLE "users" (
    "id" TEXT NOT NULL,
    "firstname" TEXT NOT NULL,
    "lastname" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "matricNumber" VARCHAR(20) NOT NULL,
    "password" TEXT NOT NULL,
    "role" "Role" NOT NULL DEFAULT 'STUDENT',
    "isVerified" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "wallets" (
    "id" TEXT NOT NULL,
    "balance" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "is_linked" BOOLEAN NOT NULL DEFAULT false,
    "v_account_number" VARCHAR(20),
    "v_bank_name" VARCHAR(50),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,

    CONSTRAINT "wallets_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "terminals" (
    "terminal_id" VARCHAR(20) NOT NULL,
    "status" "TerminalStatus" NOT NULL DEFAULT 'OFFLINE',
    "last_seen" TIMESTAMPTZ(6),
    "active_driver_uid" VARCHAR(20),
    "secret_key" VARCHAR(64) NOT NULL,

    CONSTRAINT "terminals_pkey" PRIMARY KEY ("terminal_id")
);

-- CreateTable
CREATE TABLE "transactions" (
    "transaction_id" VARCHAR(64) NOT NULL,
    "type" "TransactionType" NOT NULL DEFAULT 'RIDE',
    "terminal_id" VARCHAR(20) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "amount" DECIMAL(10,2) NOT NULL,
    "driver_uid" VARCHAR(20),
    "synced_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "transactions_pkey" PRIMARY KEY ("transaction_id")
);

-- CreateTable
CREATE TABLE "kyc_records" (
    "id" TEXT NOT NULL,
    "idCardImageUrl" TEXT NOT NULL,
    "status" "KycStatus" NOT NULL DEFAULT 'PENDING',
    "rejectionReason" TEXT,
    "submittedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "reviewedAt" TIMESTAMP(3),
    "userId" TEXT NOT NULL,

    CONSTRAINT "kyc_records_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "blacklist" (
    "id" TEXT NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "reason" TEXT NOT NULL DEFAULT 'LOW_BALANCE',
    "blacklistedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "blacklist_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "card_mappings" (
    "id" TEXT NOT NULL,
    "card_uid" VARCHAR(20) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "linked_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "card_mappings_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "agents" (
    "id" TEXT NOT NULL,
    "firstname" TEXT NOT NULL,
    "lastname" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "phone" VARCHAR(15) NOT NULL,
    "password" TEXT NOT NULL,
    "status" "AgentStatus" NOT NULL DEFAULT 'ACTIVE',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "createdBy" TEXT NOT NULL,

    CONSTRAINT "agents_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "notifications" (
    "id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "isRead" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "student_uid" VARCHAR(20) NOT NULL,

    CONSTRAINT "notifications_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "disputes" (
    "id" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "status" "DisputeStatus" NOT NULL DEFAULT 'OPEN',
    "resolution" TEXT,
    "resolvedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "transaction_id" VARCHAR(64) NOT NULL,
    "resolvedByAgent" TEXT,
    "resolvedByAdmin" TEXT,

    CONSTRAINT "disputes_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "registration_otps" (
    "id" TEXT NOT NULL,
    "otp" TEXT NOT NULL,
    "card_uid" VARCHAR(20) NOT NULL,
    "terminal_id" VARCHAR(20) NOT NULL,
    "expires_at" TIMESTAMP(3) NOT NULL,
    "used" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "registration_otps_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "refresh_tokens" (
    "id" TEXT NOT NULL,
    "token_id" TEXT NOT NULL,
    "user_id" TEXT NOT NULL,
    "role" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "expires_at" TIMESTAMP(3) NOT NULL,
    "revoked" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "refresh_tokens_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");

-- CreateIndex
CREATE UNIQUE INDEX "users_matricNumber_key" ON "users"("matricNumber");

-- CreateIndex
CREATE INDEX "users_role_isVerified_idx" ON "users"("role", "isVerified");

-- CreateIndex
CREATE UNIQUE INDEX "wallets_v_account_number_key" ON "wallets"("v_account_number");

-- CreateIndex
CREATE UNIQUE INDEX "wallets_student_uid_key" ON "wallets"("student_uid");

-- CreateIndex
CREATE INDEX "wallets_is_linked_idx" ON "wallets"("is_linked");

-- CreateIndex
CREATE INDEX "terminals_status_idx" ON "terminals"("status");

-- CreateIndex
CREATE INDEX "transactions_student_uid_synced_at_idx" ON "transactions"("student_uid", "synced_at" DESC);

-- CreateIndex
CREATE INDEX "transactions_terminal_id_synced_at_idx" ON "transactions"("terminal_id", "synced_at" DESC);

-- CreateIndex
CREATE INDEX "transactions_type_idx" ON "transactions"("type");

-- CreateIndex
CREATE INDEX "transactions_driver_uid_synced_at_idx" ON "transactions"("driver_uid", "synced_at" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "kyc_records_userId_key" ON "kyc_records"("userId");

-- CreateIndex
CREATE INDEX "kyc_records_status_idx" ON "kyc_records"("status");

-- CreateIndex
CREATE INDEX "kyc_records_status_submittedAt_idx" ON "kyc_records"("status", "submittedAt" ASC);

-- CreateIndex
CREATE UNIQUE INDEX "blacklist_student_uid_key" ON "blacklist"("student_uid");

-- CreateIndex
CREATE INDEX "blacklist_blacklistedAt_idx" ON "blacklist"("blacklistedAt" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "card_mappings_card_uid_key" ON "card_mappings"("card_uid");

-- CreateIndex
CREATE UNIQUE INDEX "card_mappings_student_uid_key" ON "card_mappings"("student_uid");

-- CreateIndex
CREATE INDEX "card_mappings_linked_at_idx" ON "card_mappings"("linked_at" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "agents_email_key" ON "agents"("email");

-- CreateIndex
CREATE INDEX "agents_status_idx" ON "agents"("status");

-- CreateIndex
CREATE INDEX "agents_createdBy_status_idx" ON "agents"("createdBy", "status");

-- CreateIndex
CREATE INDEX "notifications_student_uid_createdAt_idx" ON "notifications"("student_uid", "createdAt" DESC);

-- CreateIndex
CREATE INDEX "notifications_student_uid_isRead_idx" ON "notifications"("student_uid", "isRead");

-- CreateIndex
CREATE INDEX "disputes_status_createdAt_idx" ON "disputes"("status", "createdAt" ASC);

-- CreateIndex
CREATE INDEX "disputes_student_uid_createdAt_idx" ON "disputes"("student_uid", "createdAt" DESC);

-- CreateIndex
CREATE INDEX "disputes_transaction_id_idx" ON "disputes"("transaction_id");

-- CreateIndex
CREATE UNIQUE INDEX "registration_otps_otp_key" ON "registration_otps"("otp");

-- CreateIndex
CREATE INDEX "registration_otps_otp_idx" ON "registration_otps"("otp");

-- CreateIndex
CREATE INDEX "registration_otps_expires_at_idx" ON "registration_otps"("expires_at");

-- CreateIndex
CREATE UNIQUE INDEX "refresh_tokens_token_id_key" ON "refresh_tokens"("token_id");

-- CreateIndex
CREATE INDEX "refresh_tokens_token_id_idx" ON "refresh_tokens"("token_id");

-- CreateIndex
CREATE INDEX "refresh_tokens_expires_at_idx" ON "refresh_tokens"("expires_at");

-- AddForeignKey
ALTER TABLE "wallets" ADD CONSTRAINT "wallets_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_terminal_id_fkey" FOREIGN KEY ("terminal_id") REFERENCES "terminals"("terminal_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "kyc_records" ADD CONSTRAINT "kyc_records_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "agents" ADD CONSTRAINT "agents_createdBy_fkey" FOREIGN KEY ("createdBy") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "notifications" ADD CONSTRAINT "notifications_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "disputes" ADD CONSTRAINT "disputes_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "disputes" ADD CONSTRAINT "disputes_transaction_id_fkey" FOREIGN KEY ("transaction_id") REFERENCES "transactions"("transaction_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "disputes" ADD CONSTRAINT "disputes_resolvedByAgent_fkey" FOREIGN KEY ("resolvedByAgent") REFERENCES "agents"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "disputes" ADD CONSTRAINT "disputes_resolvedByAdmin_fkey" FOREIGN KEY ("resolvedByAdmin") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
