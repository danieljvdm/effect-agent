import { Option, Schema } from "effect";

const Label = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(240));
const Detail = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(600));

const PublicUrl = Schema.URLFromString.check(
  Schema.makeFilter(
    (url) =>
      url.protocol === "https:" &&
      url.username === "" &&
      url.password === "" &&
      url.port === "" &&
      url.hostname.includes(".") &&
      !url.hostname.endsWith(".") &&
      !/^[\d.]+$/.test(url.hostname) &&
      !/(?:^|\.)(?:localhost|local|internal|invalid|test)$/.test(url.hostname),
    { title: "a public HTTPS URL without credentials or a custom port" },
  ),
);

/** Model-supplied links never become HTML or server-side fetch targets. */
export const TravelUrl = Schema.String.check(
  Schema.isMaxLength(2_048),
  Schema.makeFilter((value) => Option.isSome(Schema.decodeUnknownOption(PublicUrl)(value)), {
    title: "a public HTTPS URL",
  }),
);

export const safeTravelUrl = (value: string | undefined): string | undefined =>
  value !== undefined && Schema.is(TravelUrl)(value) ? value : undefined;

export const TravelPhoto = Schema.Struct({ url: TravelUrl, caption: Label });
export type TravelPhoto = typeof TravelPhoto.Type;

export const StayOption = Schema.Struct({
  kind: Schema.Literal("stay"),
  name: Label,
  location: Label,
  url: TravelUrl,
  photos: Schema.Array(TravelPhoto).check(Schema.isMaxLength(4)),
  highlights: Schema.Array(Label).check(Schema.isMaxLength(6)),
  price: Schema.NullOr(Label),
  note: Schema.NullOr(Detail),
});

export type StayOption = typeof StayOption.Type;

export const FlightOption = Schema.Struct({
  kind: Schema.Literal("flight"),
  airline: Label,
  origin: Label,
  destination: Label,
  departure: Schema.NullOr(Label),
  arrival: Schema.NullOr(Label),
  duration: Schema.NullOr(Label),
  stops: Schema.NullOr(Label),
  price: Schema.NullOr(Label),
  url: TravelUrl,
  note: Schema.NullOr(Detail),
});

export type FlightOption = typeof FlightOption.Type;

export const ItineraryOption = Schema.Struct({
  kind: Schema.Literal("itinerary"),
  title: Label,
  days: Schema.Array(
    Schema.Struct({
      label: Label,
      date: Schema.NullOr(Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/))),
      activities: Schema.Array(
        Schema.Struct({
          time: Schema.NullOr(Label),
          title: Label,
          description: Schema.NullOr(Detail),
          url: Schema.NullOr(TravelUrl),
        }),
      ).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
    }),
  ).check(Schema.isMinLength(1), Schema.isMaxLength(14)),
});

export type ItineraryOption = typeof ItineraryOption.Type;

export const PlaceOption = Schema.Struct({
  kind: Schema.Literal("place"),
  name: Label,
  category: Schema.Literals(["restaurant", "activity", "sight"]),
  location: Label,
  description: Detail,
  url: TravelUrl,
  photo: Schema.NullOr(TravelPhoto),
});

export type PlaceOption = typeof PlaceOption.Type;

/** A display-only tool result, persisted by the existing canonical tool-result log. */
export const TravelContent = Schema.Struct({
  title: Label,
  items: Schema.Array(Schema.Union([StayOption, FlightOption, ItineraryOption, PlaceOption])).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(6),
  ),
}).check(
  Schema.makeFilter(
    (value) => new TextEncoder().encode(JSON.stringify(value)).byteLength <= 16 * 1_024,
    {
      title: "travel cards within a 16 KiB display budget",
    },
  ),
);

export type TravelContent = typeof TravelContent.Type;
