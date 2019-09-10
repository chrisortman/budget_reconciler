NimbleCSV.define(HillsParser, separator: ",", escape: "\"")
NimbleCSV.define(ChaseParser, separator: ",")

defmodule BudgetReconciler do
  use Timex

  alias BudgetReconciler.Filters

  @moduledoc """
  Documentation for BudgetReconciler.
  """

  def ynab_data(which) do
    {:ok, data} = File.read("ynab_#{which}_data.json")
    ynab = Jason.decode!(data)
    transactions = get_in(ynab, ["data", "transactions"])

    Enum.map(transactions, fn d ->
      %{
        source: :ynab,
        amount: d["amount"],
        account_name: d["account_name"],
        approved: d["approved"],
        cleared: d["cleared"],
        date: Timex.parse!(d["date"], "{YYYY}-{0M}-{0D}") |> Timex.to_date(),
        deleted: d["deleted"],
        import_id: d["import_id"],
        memo: d["memo"],
        payee_name: d["payee_name"]
      }
    end)
  end

  def hills_data() do
    {:ok, data} = File.read("hills_data.csv")

    data
    |> HillsParser.parse_string()
    |> Enum.map(fn [account_number, post_date, check, description, debit, credit, status, balance] ->
      dt = Timex.parse!(post_date, "{M}/{D}/{YYYY}") |> Timex.to_date()

      amount =
        if credit == "" do
          parse_hills_amount(debit) * 1000 * -1
        else
          parse_hills_amount(credit) * 1000
        end

      %{
        source: :hills,
        account_number: account_number,
        post_date: dt,
        check: check,
        description: description,
        debit: debit,
        amount: Kernel.trunc(amount),
        credit: credit,
        status: status,
        balance: balance
      }
    end)
  end

  def chase_data() do
    {:ok, data} = File.read("chase_data.csv")

    data
    |> ChaseParser.parse_string()
    |> Enum.map(fn [transaction_date, post_date, description, category, type, amount] ->
      %{
        source: :chase,
        transaction_date: Timex.parse!(transaction_date, "{0M}/{0D}/{YYYY}") |> Timex.to_date(),
        post_date: Timex.parse!(post_date, "{0M}/{0D}/{YYYY}") |> Timex.to_date(),
        description: description,
        category: category,
        type: type,
        amount: parse_chase_amount(amount)
      }
    end)
  end

  def record_string(%{:source => :ynab} = record) do
    fields = [record.date, amount_string(record.amount), record.payee_name, "\n"]
    Enum.join(fields, " ")
  end

  def record_string(record) do
    fields = [record.post_date, amount_string(record.amount), record.description]
    Enum.join(fields, " ")
  end

  def amount_string(amount) do
    amount = amount / 1000
    amount = Float.round(amount,2)
    to_string(amount)
  end

  defp parse_chase_amount(string) do
    case Float.parse(string) do
      {num, ""} ->
        (num * 1000) |> Kernel.trunc()

      _ ->
        IO.puts("Cant parse '#{string}'")
        0
    end
  end

  defp parse_hills_amount("." <> other), do: parse_hills_amount("0." <> other)

  defp parse_hills_amount(string) when is_binary(string) do
    case Float.parse(string) do
      {num, ""} ->
        num

      _ ->
        IO.puts("Cant parse '#{string}'")
        0
    end
  end

  def compare(what, days_diff \\ 2)
  def compare(account, days_diff) when is_atom(account) do
    {other_name, data} = case account do
      :hills ->
        ynab = ynab_data("hills")
        hills = hills_data()
        {"Hills", {ynab,hills}}
      :chase ->
        ynab = ynab_data("chase")
        chase = chase_data()
        {"Chase", {ynab,chase}}
    end

    compare(other_name, data, days_diff)
  end

  def compare(other_name, {ynab, other}, days_diff) do
    min_date = Enum.min_by(ynab, fn x -> x[:date] end) |> Map.get(:date)
    max_date = Enum.max_by(ynab, fn x -> x[:date] end) |> Map.get(:date)

    other_in_date  = Enum.filter(other, Filters.between(min_date, max_date))

    {in_other, not_in_other} = Enum.split_with(ynab, Filters.in_other(other, days_diff))
    {in_ynab, not_in_ynab} = Enum.split_with(other_in_date , Filters.in_ynab(ynab, days_diff))

    IO.puts("#{Enum.count(ynab)} records in YNAB from #{min_date} to #{max_date}")
    IO.puts("#{Enum.count(other)} records in #{other_name} (#{Enum.count(other_in_date)}) from #{min_date} to #{max_date}")

    IO.puts("")
    IO.puts("############################################")
    IO.puts("       NOT IN #{other_name} BANK                    ")
    IO.puts("")
    not_in_other |> Enum.map(&record_string/1) |> IO.puts
    IO.puts("#{Enum.count(not_in_other)} records not in #{other_name}")
    IO.puts("#{Enum.count(in_other)} records in #{other_name}")
    IO.puts("")
    IO.puts("")
    IO.puts("############################################")
    IO.puts("               NOT IN YNAB                  ")
    IO.puts("")
    IO.puts("")
    not_in_ynab |> Enum.map(&record_string/1) |> IO.puts
    IO.puts("#{Enum.count(not_in_ynab)} records not in YNAB")
    IO.puts("#{Enum.count(in_ynab)} records in YNAB")

    
    :ok
  end


  defmodule Filters do

    def reconciled?() do
      fn ynab_tx ->
        ynab_tx[:cleared] == "reconciled"
      end
    end

    def not_reconciled?() do
      fn ynab_tx ->
        ynab_tx[:cleared] != "reconciled"
      end
    end

    def with_amount(amount) do
      fn tx ->
        tx[:amount] == amount
      end
    end

    def between(start_date \\ ~D[2019-03-01], end_date \\ ~D[2019-09-01]) do
      fn hills_tx ->
        starts_after = Date.compare(hills_tx[:post_date], start_date) in [:gt, :eq]
        starts_before = Date.compare(hills_tx[:post_date], end_date) in [:lt, :eq]

        starts_after && starts_before
      end
    end

    def in_other(other, days_diff \\ 2) do
      fn ynab_tx ->
        Enum.any?(other, &is_same?(ynab_tx, &1, days_diff))
      end
    end

    def in_ynab(ynab, days_diff \\ 2) do
      fn other_tx ->
        Enum.any?(ynab, &is_same?(&1, other_tx, days_diff))
      end
    end

    def is_same?(ynab_tx, hills_tx, days_diff_limit \\ 2) do
      if abs(ynab_tx.amount - hills_tx.amount) <= 1 do
        days_diff = Timex.diff(ynab_tx.date, hills_tx.post_date, :days) |> abs()

        days_diff <= days_diff_limit
      else
        false
      end
    end
  end

end
