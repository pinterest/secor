#!/usr/bin/env bash

########################################################################################################################
#
# Copyright © 2017 Unified Social, Inc.
# 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
# All rights reserved.
#
# This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
# Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
# Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
# EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
#
########################################################################################################################


# ======================================================================================================================
# CONFIGURATION
# ======================================================================================================================

# ----------  Global configuration  ----------

[ -z "${ENVIRONMENT_TYPE}" ] && declare ENVIRONMENT_TYPE="stage"


# ----------  Jenkins  ----------

[ -z "${DIR_ROOT}" ] && declare DIR_ROOT=$( cd `dirname "${0}"` && pwd )
[ -z "${DIR_BASE}" ] && declare DIR_BASE="${DIR_ROOT}/../../../../../Base"

source "${DIR_BASE}/src/main/scripts/build/jenkins.sh"


# ----------  Local configuration  ----------

declare -a JOBS=(                       \
    Secor_Pinterest_02_Snapshot         \
    Secor_Pinterest_04_Package          \
    Secor_Pinterest_05_Provision        \
    Secor_Pinterest_06_Provision_Update \
    Secor_Pinterest_07_Deploy           \
    Secor_Pinterest_08_Manage_Service   \
    Secor_Pinterest_09_Deprovision      \
)


# ======================================================================================================================
# MAIN
# ======================================================================================================================

main_loop "$1" "Secor_Pinterest" JOBS "$2"


# ======================================================================================================================
