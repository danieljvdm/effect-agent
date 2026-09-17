import { Effect, Record, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

// These eight are the application default, chosen independently of the tasks.
export const commonTools = [
  "get_customer_profile",
  "get_order_shipping",
  "get_invoice",
  "get_product",
  "get_support_ticket",
  "get_account_balance",
  "get_subscription",
  "get_order_items",
] as const;

export const catalogue = {
  get_customer_profile: "Read customer name, contact details and account tier by customer ID.",
  get_order_shipping: "Read current shipping status and tracking code by order ID.",
  get_invoice: "Read invoice total, currency and payment status by invoice ID.",
  get_product: "Read product name, description and list price by product ID.",
  get_support_ticket: "Read a support ticket and its linked operational record by ticket ID.",
  get_account_balance: "Read current available balance by account ID.",
  get_subscription: "Read subscription plan, renewal date and active status by subscription ID.",
  get_order_items: "Read line items and quantities by order ID.",
  get_customer_addresses: "Read saved billing and shipping addresses by customer ID.",
  get_customer_preferences: "Read communication and language preferences by customer ID.",
  get_customer_loyalty: "Read loyalty points and rewards tier by customer ID.",
  get_customer_consent: "Read marketing consent records by customer ID.",
  get_order_payment: "Read the payment method and charge status by order ID.",
  get_order_return: "Read return authorization and returned items by order ID.",
  get_order_refund: "Read refund amount and processing status by order ID.",
  get_order_discounts: "Read applied promotions and discount amounts by order ID.",
  get_invoice_tax: "Read invoice tax jurisdiction and assessed tax by invoice ID.",
  get_invoice_credit: "Read credit notes and adjustments by invoice ID.",
  get_invoice_reminders: "Read payment reminder history by invoice ID.",
  get_invoice_allocation: "Read cost-center accounting allocation by invoice ID.",
  get_product_inventory: "Read warehouse quantities and reserved stock by product ID.",
  get_product_supplier: "Read supplier contact and lead time by product ID.",
  get_product_warranty: "Read warranty terms and coverage by product ID.",
  get_product_recall: "Read active safety recalls by product ID.",
  get_ticket_comments: "Read support ticket conversation comments by ticket ID.",
  get_ticket_sla: "Read service-level deadlines and breach status by ticket ID.",
  get_ticket_assignment: "Read assigned team and support representative by ticket ID.",
  get_ticket_attachments: "Read attachment names and document references by ticket ID.",
  get_account_transactions: "Read recent ledger transactions by account ID.",
  get_account_limits: "Read transfer and withdrawal limits by account ID.",
  get_account_statement: "Read the latest statement summary by account ID.",
  get_account_verification: "Read identity verification status by account ID.",
  get_subscription_usage: "Read metered usage and quota consumption by subscription ID.",
  get_subscription_addons: "Read enabled optional features by subscription ID.",
  get_subscription_cancellation: "Read cancellation request and scheduled end by subscription ID.",
  get_subscription_history: "Read previous plan changes by subscription ID.",
  get_retention_exception:
    "Read compliance data-retention exemption code and expiry by customer ID.",
  get_privacy_export: "Read personal-data export request status by customer ID.",
  get_privacy_deletion: "Read personal-data deletion request status by customer ID.",
  get_compliance_audit: "Read compliance audit findings by audit ID.",
  get_sensor_calibration:
    "Read current calibration status and verification code by calibration record ID.",
  get_warehouse_capacity: "Read storage capacity and utilization by warehouse ID.",
  get_warehouse_maintenance: "Read planned equipment maintenance by warehouse ID.",
  get_warehouse_temperature: "Read recent temperature measurements by warehouse ID.",
  get_shipment_customs: "Read customs clearance and duties by shipment ID.",
  get_shipment_insurance: "Read declared value and insurance coverage by shipment ID.",
  get_employee_schedule: "Read assigned shifts by employee ID.",
  get_employee_training: "Read completed training certifications by employee ID.",
  get_vendor_contract: "Read contract term and purchasing conditions by vendor ID.",
  get_vendor_rating: "Read supplier quality and delivery rating by vendor ID.",
} as const;

export const Parameters = Schema.Struct({ id: Schema.NonEmptyString });
export const ToolResult = Schema.Struct({ value: Schema.String });
export const Output = Schema.Struct({ answer: Schema.String });

export const tools = Record.toEntries(catalogue).map(([name, description]) =>
  Tool.make(name, { description, parameters: Parameters, success: ToolResult }).annotate(
    Tool.Readonly,
    true,
  ),
);

export const toolkit = Toolkit.make(...tools);

const records: Readonly<Record<string, string>> = {
  "get_order_shipping/ORD-104": "status=in_transit; tracking=TRACK-Q7M4",
  "get_retention_exception/CUS-204": "exemption=RET-X29F; expiry=2027-03-31",
  "get_support_ticket/TKT-309":
    "The linked operational record is calibration CAL-77. Use get_sensor_calibration with id CAL-77 to obtain its current status and verification code.",
  "get_sensor_calibration/CAL-77": "status=calibration_due; verification=CAL-V6P8",
};

export const makeHandlers = (observe: (name: string, id: string) => Effect.Effect<void>) =>
  toolkit.toLayer(
    Record.map(
      catalogue,
      (_description, name) =>
        ({ id }: typeof Parameters.Type) =>
          observe(name, id).pipe(
            Effect.as({ value: records[`${name}/${id}`] ?? "No matching record." }),
          ),
    ),
  );

export const tasks = [
  {
    name: "common",
    input: "What are the current shipping status and tracking code for order ORD-104?",
    evidence: ["in_transit", "TRACK-Q7M4"],
    requiredCalls: ["get_order_shipping/ORD-104"],
  },
  {
    name: "rare",
    input: "What are the compliance data-retention exemption code and expiry for customer CUS-204?",
    evidence: ["RET-X29F", "2027-03-31"],
    requiredCalls: ["get_retention_exception/CUS-204"],
  },
  {
    name: "followup",
    input:
      "Investigate support ticket TKT-309. Follow its linked operational record and report that record's current status and verification code.",
    evidence: ["calibration_due", "CAL-V6P8"],
    requiredCalls: ["get_support_ticket/TKT-309", "get_sensor_calibration/CAL-77"],
  },
] as const;

export type Task = (typeof tasks)[number];
