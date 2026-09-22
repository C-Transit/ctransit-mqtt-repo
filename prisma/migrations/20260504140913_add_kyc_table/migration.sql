/*
  Warnings:

  - The `status` column on the `terminals` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `type` column on the `transactions` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - A unique constraint covering the columns `[studentId]` on the table `kyc_records` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateEnum
CREATE TYPE "TransactionType" AS ENUM ('RIDE', 'TOPUP', 'REFUND');

-- CreateEnum
CREATE TYPE "TerminalStatus" AS ENUM ('ONLINE', 'OFFLINE', 'LOCKED');

-- AlterTable
ALTER TABLE "terminals" DROP COLUMN "status",
ADD COLUMN     "status" "TerminalStatus" NOT NULL DEFAULT 'OFFLINE';

-- AlterTable
ALTER TABLE "transactions" DROP COLUMN "type",
ADD COLUMN     "type" "TransactionType" NOT NULL DEFAULT 'RIDE';

-- CreateIndex
CREATE UNIQUE INDEX "kyc_records_studentId_key" ON "kyc_records"("studentId");
