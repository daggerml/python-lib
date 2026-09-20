import neutral from "../assets/icons/daggerml-icons-v2/128/neutral.png";
import happy from "../assets/icons/daggerml-icons-v2/128/happy.png";
import tweaking from "../assets/icons/daggerml-icons-v2/128/tweaking.png";
import xEyes from "../assets/icons/daggerml-icons-v2/128/x-eyes.png";

const expressions = { neutral, happy, tweaking, "x-eyes": xEyes };

/** Decorative mascot; the adjacent label communicates the brand or state. */
export function BrandIcon({ expression = "neutral", size = 32, className = "" }: {
  expression?: keyof typeof expressions;
  size?: 24 | 32 | 64;
  className?: string;
}) {
  return <img className={`brand-icon ${className}`} src={expressions[expression]} width={size} height={size} alt="" aria-hidden="true" />;
}
