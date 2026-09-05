---
"@effect-agent/testing": minor
---

Remove the unused Travel Planner phase-profile schemas and metadata constants.

BEHAVIOR CHANGE: Remove imports of `TravelPlanner*Profile`, `phase3TravelPlannerProfile` through
`phase7TravelPlannerProfile`, and `s2TravelPlannerProfile` from `@effect-agent/testing/TravelPlanner`;
the executable fixtures and live-test gate remain available.
