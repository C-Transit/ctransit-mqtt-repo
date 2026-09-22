/*
  Warnings:

  - You are about to drop the column `department` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `faceImageUrl` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `matricNumber` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `phoneNumber` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `school` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `studentId` on the `kyc_records` table. All the data in the column will be lost.
  - You are about to drop the column `studentName` on the `kyc_records` table. All the data in the column will be lost.

*/
-- DropIndex
DROP INDEX "kyc_records_studentId_key";

-- AlterTable
ALTER TABLE "kyc_records" DROP COLUMN "department",
DROP COLUMN "faceImageUrl",
DROP COLUMN "matricNumber",
DROP COLUMN "phoneNumber",
DROP COLUMN "school",
DROP COLUMN "studentId",
DROP COLUMN "studentName";
