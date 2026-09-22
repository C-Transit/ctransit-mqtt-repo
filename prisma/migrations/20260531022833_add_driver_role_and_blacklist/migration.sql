-- AlterEnum
ALTER TYPE "Role" ADD VALUE 'DRIVER';

-- CreateTable
CREATE TABLE "blacklist" (
    "id" TEXT NOT NULL,
    "student_uid" VARCHAR(20) NOT NULL,
    "reason" TEXT NOT NULL DEFAULT 'LOW_BALANCE',
    "blacklistedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "blacklist_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "blacklist_student_uid_key" ON "blacklist"("student_uid");
