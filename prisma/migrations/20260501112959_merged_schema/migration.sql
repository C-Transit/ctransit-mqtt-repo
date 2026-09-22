/*
  Warnings:

  - You are about to drop the `Transaction` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `User` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "Transaction";

-- DropTable
DROP TABLE "User";

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
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,

    CONSTRAINT "wallets_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "terminals" (
    "terminal_id" VARCHAR(20) NOT NULL,
    "status" VARCHAR(20) NOT NULL DEFAULT 'OFFLINE',
    "last_seen" TIMESTAMPTZ(6),
    "active_driver_uid" VARCHAR(20),
    "secret_key" VARCHAR(64) NOT NULL,

    CONSTRAINT "terminals_pkey" PRIMARY KEY ("terminal_id")
);

-- CreateTable
CREATE TABLE "transactions" (
    "transaction_id" VARCHAR(64) NOT NULL,
    "type" VARCHAR(10) NOT NULL DEFAULT 'RIDE',
    "terminal_id" VARCHAR(20) NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "amount" DECIMAL(10,2) NOT NULL,
    "driver_uid" VARCHAR(20),
    "synced_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "transactions_pkey" PRIMARY KEY ("transaction_id")
);

-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");

-- CreateIndex
CREATE UNIQUE INDEX "users_matricNumber_key" ON "users"("matricNumber");

-- CreateIndex
CREATE UNIQUE INDEX "wallets_student_uid_key" ON "wallets"("student_uid");

-- AddForeignKey
ALTER TABLE "wallets" ADD CONSTRAINT "wallets_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_student_uid_fkey" FOREIGN KEY ("student_uid") REFERENCES "users"("matricNumber") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "transactions" ADD CONSTRAINT "transactions_terminal_id_fkey" FOREIGN KEY ("terminal_id") REFERENCES "terminals"("terminal_id") ON DELETE RESTRICT ON UPDATE CASCADE;
