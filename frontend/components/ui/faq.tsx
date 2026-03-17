"use client";

import * as React from "react";
import { AnimatePresence, motion } from "framer-motion";
import { ChevronDown, Mail } from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface FaqSectionProps extends React.HTMLAttributes<HTMLElement> {
  title: string;
  description?: string;
  items: {
    question: string;
    answer: string;
  }[];
  contactInfo?: {
    title: string;
    description: string;
    buttonText: string;
    onContact?: () => void;
  };
}

const FaqSection = React.forwardRef<HTMLElement, FaqSectionProps>(
  ({ className, title, description, items, contactInfo, ...props }, ref) => {
    return (
      <section
        ref={ref}
        className={cn(
          "w-full bg-gradient-to-b from-transparent via-[#F8F8FB]/70 to-transparent py-16",
          className
        )}
        {...props}
      >
        <div className="app-container">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, amount: 0.4 }}
            transition={{ duration: 0.5 }}
            className="mx-auto mb-12 max-w-2xl text-center"
          >
            <h2 className="text-3xl font-medium tracking-[-0.03em] text-[#0A0A0A] sm:text-4xl">
              {title}
            </h2>
            {description ? (
              <p className="mt-3 text-sm leading-6 text-[#6B6B6B] sm:text-base">
                {description}
              </p>
            ) : null}
          </motion.div>

          <div className="mx-auto max-w-2xl space-y-2">
            {items.map((item, index) => (
              <FaqItem
                key={item.question}
                question={item.question}
                answer={item.answer}
                index={index}
              />
            ))}
          </div>

          {contactInfo ? (
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.4 }}
              transition={{ duration: 0.5, delay: 0.2 }}
              className="mx-auto mt-12 max-w-md rounded-[20px] border border-[#E6E6E6] bg-white p-6 text-center shadow-[0_10px_30px_rgba(0,0,0,0.05)]"
            >
              <div className="mb-4 inline-flex items-center justify-center rounded-full border border-[#E7D8FB] bg-[#F5EEFC] p-2 text-[#8B50D4]">
                <Mail className="h-4 w-4" />
              </div>
              <p className="mb-1 text-sm font-medium text-[#0A0A0A]">
                {contactInfo.title}
              </p>
              <p className="mb-4 text-xs leading-5 text-[#6B6B6B]">
                {contactInfo.description}
              </p>
              <Button
                size="sm"
                onClick={contactInfo.onContact}
                className="border border-transparent bg-primary text-white hover:brightness-95"
              >
                {contactInfo.buttonText}
              </Button>
            </motion.div>
          ) : null}
        </div>
      </section>
    );
  }
);

FaqSection.displayName = "FaqSection";

const FaqItem = React.forwardRef<
  HTMLDivElement,
  {
    question: string;
    answer: string;
    index: number;
  }
>(({ question, answer, index }, ref) => {
  const [isOpen, setIsOpen] = React.useState(false);

  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 10 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, amount: 0.35 }}
      transition={{ duration: 0.2, delay: index * 0.06 }}
      className={cn(
        "group rounded-[16px] border border-[#E6E6E6] bg-white transition-all duration-200 ease-in-out shadow-[0_1px_3px_rgba(0,0,0,0.05)]",
        isOpen ? "bg-[linear-gradient(180deg,#FFFFFF_0%,#FBF8FF_100%)]" : "hover:bg-[#FCFAFF]"
      )}
    >
      <Button
        variant="ghost"
        onClick={() => setIsOpen(!isOpen)}
        className="h-auto w-full items-start justify-between gap-3 px-6 py-4 text-left hover:bg-transparent"
      >
        <h3
          className={cn(
            "min-w-0 flex-1 whitespace-normal break-words pr-1 text-base font-medium text-[#6B6B6B] transition-colors duration-200",
            isOpen && "text-[#0A0A0A]"
          )}
        >
          {question}
        </h3>
        <motion.div
          animate={{ rotate: isOpen ? 180 : 0, scale: isOpen ? 1.08 : 1 }}
          transition={{ duration: 0.2 }}
          className={cn(
            "flex-shrink-0 rounded-full p-0.5 transition-colors duration-200",
            isOpen ? "text-[#8B50D4]" : "text-[#6B6B6B]"
          )}
        >
          <ChevronDown className="h-4 w-4" />
        </motion.div>
      </Button>
      <AnimatePresence initial={false}>
        {isOpen ? (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{
              height: "auto",
              opacity: 1,
              transition: { duration: 0.2, ease: "easeOut" },
            }}
            exit={{
              height: 0,
              opacity: 0,
              transition: { duration: 0.2, ease: "easeIn" },
            }}
          >
            <div className="px-6 pb-5 pt-1">
              <motion.p
                initial={{ y: -10, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                exit={{ y: -10, opacity: 0 }}
                className="text-sm leading-relaxed text-[#6B6B6B]"
              >
                {answer}
              </motion.p>
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </motion.div>
  );
});

FaqItem.displayName = "FaqItem";

export { FaqSection };
