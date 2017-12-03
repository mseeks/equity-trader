class AddEquities < ActiveRecord::Migration[5.0]
  def change
    create_table :equities do |t|
      t.string :symbol, index: true, null: false, unique: true
      t.integer :signal, null: false, default: 0

      t.timestamps
    end
  end
end
