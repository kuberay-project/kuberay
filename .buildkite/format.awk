###############################################################################
# This AWK script processes lines from test logs.
#
# - If a line starts with "=== RUN":
#     1) Split the third field on the "/" delimiter to check if it's a subtest.
#        (e.g., $3 might be "Test/SomeSubtest")
#     2) If it is a subtest, print as is.
#        (=== RUN Test/SomeSubtest)
#     3) Otherwise, replace "===" with "---" (indicating a main test).
#        (--- RUN Test)
#
# - If a line contains "---" but does NOT contain "RUN", we interpret it as
#   non-RUN log lines. We replace "---" with "###".
#   ("--- PASS" to "### PASS")
#
# - All other lines are printed unchanged.
###############################################################################

# Match lines starting with "=== RUN"
/^=== RUN/ {

    # Split the 3rd field on "/"
    split($3, parts, "/")

    # Check if more than one part was created after splitting on "/"
    # This indicates it's a subtest (e.g., "Test/SomeSubtest").
    if (length(parts) > 1) {
        # For subtests, print the line unchanged
        print
    } else {
        # If not a subtest, it's the main test. Replace "===" with "---".
        gsub(/===/, "---")
        print
    }

    # Skip any further rules for this line
    next
}

# Match lines containing "---" but not containing "RUN"
/---/ && !/RUN/ {

    # Replace "---" with "###"
    gsub(/---/, "###")
    print
    next
}

# Print all other lines unchanged
{ print }
