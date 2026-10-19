// src/services/driver.service.ts
"use strict";

import prisma from "../lib/prisma.js";
import logger from "../config/logger.js";

// Driver accounts are created by the backend. Terminals only report who is driving.
async function handleDriverLogin(terminalId: string, driverUid: string) {
  const log = logger.child({ terminalId, driverUid });

  const driver = await prisma.user.findFirst({
    where: {
      matricNumber: driverUid.toUpperCase(),
      role: "DRIVER",
    },
    select: { id: true, firstname: true, lastname: true, matricNumber: true },
  });

  if (!driver) {
    log.warn("driver.not_found_or_not_driver_role");
    return { success: false, message: "Driver not recognised." };
  }

  const updated = await prisma.terminal.updateMany({
    where: { terminal_id: terminalId },
    data: { active_driver_uid: driver.matricNumber },
  });

  if (updated.count === 0) {
    log.warn("driver.login_unknown_terminal");
    return { success: false, message: "Terminal not recognised." };
  }

  log.info("driver.logged_into_terminal");

  return {
    success: true,
    message: `Welcome ${driver.firstname}`,
    driverName: `${driver.firstname} ${driver.lastname}`,
  };
}

async function handleDriverLogout(terminalId: string, driverUid: string) {
  const log = logger.child({ terminalId, driverUid });

  await prisma.terminal.updateMany({
    where: { terminal_id: terminalId },
    data: { active_driver_uid: null },
  });

  log.info("driver.logged_out_of_terminal");
  return { success: true, message: "Driver logged out." };
}

export { handleDriverLogin, handleDriverLogout };